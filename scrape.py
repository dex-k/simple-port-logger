#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#     "requests",
#     "beautifulsoup4",
# ]
# ///

import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import os
import requests
from bs4 import BeautifulSoup

DAILY_VESSEL_MOVEMENTS_URL = "https://www.portauthoritynsw.com.au/port-operations/newcastle-harbour/newcastle-harbour-daily-vessel-movements"

HEADERS = {
    # A basic desktop UA to reduce the chance of being blocked.
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
}

logger = logging.getLogger("daily_scraper_simple")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def gen_daily_movements():
    """Generator that yields daily vessel movements."""

    logger.info("Fetching %s", DAILY_VESSEL_MOVEMENTS_URL)
    response = requests.get(DAILY_VESSEL_MOVEMENTS_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    movements = []

    table = soup.select_one(".view-vessel-movement .view-content table")
    thead = table and table.select_one("thead")
    tbody = table and table.select_one("tbody")
    if not table or not thead or not tbody:
        logger.warning("Table structure has changed or is missing.")
        return movements

    # Parse headings from the table
    headings = [th.get_text(strip=True) for th in thead.find_all("th")]
    logger.debug(f"Found table headings: {headings}")

    # Parse through each row in the table body
    movement_count = 0
    for row in tbody.select("tr"):
        columns = [td.get_text(strip=True) for td in row.find_all("td")]
        if columns:
            # Convert date and time to a datetime object
            date_str = f"{datetime.today().year} {columns[0]}"
            # No space between %b and %H
            columns[0] = datetime.strptime(date_str, "%Y %a %d %b%H:%M").replace(
                tzinfo=ZoneInfo("Australia/Sydney")
            )
            movement = dict(zip(headings, columns))
            movement_count += 1

            logger.debug(f"Parsed movement: {movement}")
            yield movement

    logger.info(f"Found {movement_count} vessel movements")


def write_to_jsonl(movements, filename):
    """Write movements to a JSONL file."""
    with open(filename, "w", encoding="utf-8") as f:
        for movement in movements:
            # Convert datetime to ISO format for JSON serialization
            movement["Date & Time"] = movement["Date & Time"].isoformat()
            f.write(json.dumps(movement) + "\n")


def create_folder_structure():
    """Create the data folder if it doesn't exist."""
    os.makedirs("data", exist_ok=True)
    year = datetime.now(ZoneInfo("Australia/Sydney")).year
    os.makedirs(f"data/{year}", exist_ok=True)
    month = datetime.now(ZoneInfo("Australia/Sydney")).month
    os.makedirs(f"data/{year}/{month:02d}", exist_ok=True)
    day = datetime.now(ZoneInfo("Australia/Sydney")).day
    os.makedirs(f"data/{year}/{month:02d}/{day:02d}", exist_ok=True)

    return f"data/{year}/{month:02d}/{day:02d}"


def main() -> None:
    try:
        movements = gen_daily_movements()
        # get current local date/time for the filename
        isostring = datetime.now(ZoneInfo("Australia/Sydney")).strftime("%F_%H%M%z")
        # ensure data folder exists
        dir = create_folder_structure()  # data/YYYY/MM/DD
        filename = f"{dir}/{isostring}.jsonl"
        write_to_jsonl(movements, filename)

    except Exception as exc:
        logger.error("Scrape failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
