#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#     "tqdm"
# ]
# ///

import copy, datetime, json, os
from zoneinfo import ZoneInfo

# use tqdm progress bar if available
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

DIRECTORY = "data/"
# DIRECTORY = "data/2025/11/12"

def get_files_in_directory(directory):
    for entry in os.scandir(directory):
        if entry.is_file():
            # yield entry.name
            yield entry.path
        elif entry.is_dir():
            yield from get_files_in_directory(os.path.join(directory, entry.name))

def filename_to_datetime(filename):
    # YYYY-MM-DD_HHMM+HHMM.EXT
    date_str = filename.split("/")[-1].split(".")[0].split("+")[0]
    date = datetime.datetime.strptime(date_str, "%Y-%m-%d_%H%M")

    return date

def get_lines_in_file(file_path):
    with open(file_path, "r") as f:
        for line in f:
            yield line

def deserialise(movement_str):
    content = json.loads(movement_str)
    date = datetime.datetime.strptime(
        content["Date & Time"].split("+")[0], 
        "%Y-%m-%dT%H:%M:%S"
    )
    content["Date & Time"] = date.replace(
        tzinfo=ZoneInfo("Australia/Sydney")
    )
    return content

def serialise(movement_obj):
    movement = copy.deepcopy(movement_obj)
    movement["Date & Time"] = movement["Date & Time"].isoformat()
    content = json.dumps(movement)
    return content + '\n'

def write_to_jsonl(movements, filename):
    """Write movements to a JSONL file."""
    with open(filename, "w", encoding="utf-8") as f:
        for movement in movements:
            f.write(serialise(movement))


def _berth_activity(moves, x, y):
    """True if the vessel left/returned to x's berth somewhere between x and y.

    A Shift counts as leaving or arriving: moving off the berth vacates it just as a
    Departure does, and moving onto it is an arrival. Ignoring shifts dropped rows for
    vessels that demonstrably did return -- tugs often move berth rather than sail out
    of the port, so their return is recorded as a Shift.
    """
    if x["ARR / DEP"] == "Arrival":
        berth = x["To"]

        def vacated(m):
            return m["From"] == berth and (
                m["ARR / DEP"] == "Departure"
                or (m["ARR / DEP"] == "Shift" and m["To"] != berth)
            )
    else:
        berth = x["From"]

        def vacated(m):
            return m["To"] == berth and (
                m["ARR / DEP"] == "Arrival"
                or (m["ARR / DEP"] == "Shift" and m["From"] != berth)
            )
    return any(
        vacated(m) and x["Date & Time"] <= m["Date & Time"] <= y["Date & Time"]
        for m in moves
    )


# A revised time is announced while the movement is still inside the schedule's ~2 week
# forward window. Measured across all 10,510 snapshots that window reaches at most 14.04
# days (p90 13.99), so a later time beyond 14 days cannot be the same movement re-timed:
# by then the earlier time had been listed, passed and left the page, and two entries
# that far apart are separate visits. Compared exactly -- `timedelta.days` truncates, so
# the obvious `gap.days > 14` silently reaches 14d23h.
MAX_SUPERSEDE = datetime.timedelta(days=14)


def drop_superseded(consolidated):
    """One row per real movement, using the port's own physical constraint.

    A vessel cannot arrive at a berth twice without departing it in between, nor depart
    twice without arriving back at it. So when two entries share a vessel, route and
    direction and nothing happened at that berth between them, they cannot both describe
    separate events: the earlier one is the same movement under a time the port later
    revised, and only the later time says what actually happened.

    Where such a movement does sit between the two, they are genuine repeat visits and
    both are kept -- collapsing those would discard real movements.
    """
    by_route = {}
    for i, movement in enumerate(consolidated):
        if movement["ARR / DEP"] not in ("Arrival", "Departure"):
            continue  # a Shift is not one half of an arrival/departure pair
        route = (movement["Vessel"], movement["From"], movement["To"], movement["ARR / DEP"])
        by_route.setdefault(route, []).append(i)
    for ids in by_route.values():
        ids.sort(key=lambda i: consolidated[i]["Date & Time"])

    by_vessel = {}
    for movement in consolidated:
        by_vessel.setdefault(movement["Vessel"], []).append(movement)

    superseded = set()
    for route, ids in by_route.items():
        if len(ids) < 2:
            continue
        moves = by_vessel[route[0]]
        for n, i in enumerate(ids):
            for j in ids[n + 1:]:
                gap = consolidated[j]["Date & Time"] - consolidated[i]["Date & Time"]
                if gap <= datetime.timedelta(0):
                    # Same instant: the port listing one movement twice with conflicting
                    # detail, not a revised time. Kept on purpose.
                    continue
                if gap > MAX_SUPERSEDE:
                    # ids are time-ordered, so every later entry is further out still.
                    break
                if not _berth_activity(moves, consolidated[i], consolidated[j]):
                    superseded.add(i)
                    break
    return [m for i, m in enumerate(consolidated) if i not in superseded]


if __name__ == "__main__":

    consolidated = [] # [0] is oldest [-1] is youngest
    consolidated_seen = set() # serialised movements already kept, to drop repeats
    future = [] # [0] is nearest [-1] is furthest

    # sort the files in the directory
    files = sorted(list(get_files_in_directory(DIRECTORY)))
    print(f"Found {len(files)} schedules to consolidate:")
    print('\n'.join(files[:2]))
    print('...')
    print('\n'.join(files[-2:]))

    print("\nConsolidating schedules...")
    # go through each schedule in order
    for file_path in tqdm(files):

        # get the date this schedule was pulled
        schedule_date = filename_to_datetime(file_path).replace(
            tzinfo=ZoneInfo("Australia/Sydney")
        )

        # split into historical and future 
        for i, movement in enumerate(future):
            if movement["Date & Time"] < schedule_date:
                # A movement already kept is dropped. The page can go on listing a
                # movement for more than one snapshot after it has passed, and when
                # several pass together their repeats interleave, so comparing only
                # against the most recent entry lets them through.
                key = serialise(movement)
                if key in consolidated_seen:
                    continue
                consolidated_seen.add(key)
                consolidated.append(movement)
            else: # chronological, so can short circuit
                break

        # whole current schedule is new future
        future = (deserialise(line) for line in get_lines_in_file(file_path))
    
    # One row per real movement: an earlier time for a movement the vessel cannot have
    # made twice is the same event under a revised time, so keep only the later one.
    kept = drop_superseded(consolidated)
    print(f"\nDropped {len(consolidated) - len(kept)} superseded movement time(s)")
    consolidated = kept

    # Update
    print(f"Generated consolidated historical schedule with {len(consolidated)} movements")
    print(f"Most newest future schedule: {files[-1]}")

    # store what we have
    write_to_jsonl(consolidated, "historical.jsonl")
    write_to_jsonl(future, "newest.jsonl")
    print("Wrote historical.jsonl and newest.jsonl to file")

