# simple-port-logger

Scrapes the daily vessel movement schedule for Newcastle Harbour, published by the
Port Authority of NSW, and keeps a historical record of it in git.

The port publishes a rolling forward schedule of arrivals, departures and shifts —
roughly two weeks ahead. This repo snapshots that page on a schedule so you can
reconstruct what the schedule *said* at a given moment, rather than only what it says
today. That's what makes it useful for studying how port activity forecasts get
revised as vessels actually arrive and depart.

## Data

Two consolidated files sit at the repo root, both JSONL (one JSON object per line):

| File | Contents |
|---|---|
| `newest.jsonl` | The most recent scrape's full schedule — mostly future movements. |
| `historical.jsonl` | Movements from earlier scrapes whose scheduled time has passed. |

Each record looks like this:

```json
{"Date & Time": "2026-10-02T13:30:00+10:00", "Expected": "N/A", "ARR / DEP": "Departure",
 "Vessel": "Nord Condor", "Vessel type": "Bulk Carrier", "Agent": "SGM",
 "From": "Dyke 4 (D4)", "To": "Xiamen", "In port": "Yes"}
```

| Field | Meaning |
|---|---|
| `Date & Time` | Scheduled time, ISO 8601 with offset, always `Australia/Sydney` |
| `Expected` | `N/A`, or an actual/estimated time when the port supplies one |
| `ARR / DEP` | `Arrival`, `Departure`, or `Shift` |
| `Vessel` | Vessel name |
| `Vessel type` | e.g. `Bulk Carrier` |
| `Agent` | Shipping agent code, e.g. `SGM` |
| `From` | Origin berth or port, e.g. `Dyke 4 (D4)`, `Xiamen` |
| `To` | Destination berth or port |
| `In port` | `Yes` / `No` |

Raw per-scrape snapshots live under `data/<year>/<month>/<day>/`, each named after the
moment the scrape was taken (`2025-11-11_1420+1100.jsonl`). The nested date folders
exist because GitHub caps files per directory, which flat storage hit after roughly ten
days of frequent scraping.

There are about 10,500 snapshots in the repo so far. The scrape rate has varied
considerably — a few hundred to around 1,600 per month, averaging several per day
recently. See Automation below for why.

## Usage

Requires [uv](https://docs.astral.sh/uv/). Both scripts declare their own dependencies
as PEP 723 inline metadata, so there is nothing to install:

```sh
uv run --script scrape.py        # fetch the current schedule into data/
uv run --script consolidate.py   # rebuild historical.jsonl and newest.jsonl from data/
```

`scrape.py` fetches the schedule page and writes one snapshot file per run.

`consolidate.py` walks every snapshot in chronological order. Movements whose scheduled
time has passed are appended to `historical.jsonl`; `newest.jsonl` is re-derived from the
last snapshot. It prints a tqdm progress bar and completes in seconds.

Both are also runnable directly (`./scrape.py`) via the `uv run` shebang.

## Automation

`.github/workflows/scheduled-scrape.yml` runs both scripts and commits any changes —
first the new files under `data/`, then the regenerated root `*.jsonl` files. Commits are
skipped when nothing changed. It can also be triggered by hand from the Actions tab.

The workflow requests a `*/5 * * * *` schedule, but GitHub throttles scheduled workflows
on free runners and the real cadence is much lower — typically a handful of runs per day,
at irregular intervals. Treat the snapshot timestamps in `data/` as the authoritative
record of when the schedule was actually captured.

## experiments/

Ad-hoc analysis scripts, not tests of the code — they interrogate the scraped data to
answer questions about port activity. They are gitignored and unversioned.

Each script has its date range and berth classification hardcoded near the top, so edit
those constants to point one at a different window:

- `test.py` — daily counts of coal-ship arrivals and departures, plotted to `plots/plot.png`
- `test-daytime-arrival.py` — the same, restricted to a 9am–5pm window
- `test2.py` — loads `historical.jsonl` into a pandas DataFrame for inspection

`test.py` and `test-daytime-arrival.py` each carry a near-identical `IS_COAL_BERTH`
mapping marking which berths handle coal. They have already drifted: the copies in
`test-daytime-arrival.py` and `test2.py` include `Carrington Wharf` and `Foreshore Park
Berth`, while `test.py` does not. A berth missing from the mapping raises `KeyError`
rather than being skipped, so these need updating when the port adds a berth. Worth
pulling into a shared module.

Generated charts land in `experiments/plots/`.

## Requirements

Python 3.11+ (for `zoneinfo`) and `uv`. `scrape.py` requests the page over HTTPS with a
desktop user-agent. If the port authority changes its markup, the table selector at
`scrape.py:45` stops matching: the script logs a warning and writes an empty (0-byte)
snapshot, which the workflow would then commit. `consolidate.py` tolerates this, since an
empty file simply contributes no movements.

## Notes

`scrape.py` resolves each movement's year from the schedule window rather than from the
current date. The page prints no year, and the schedule is a rolling ~2 week forward view,
so a January date seen in a December run belongs to the following year — taking the
current year would date it a year in the past. The schedule source is always
`Australia/Sydney`, and the script pins that zone explicitly, so the resolved year does not
depend on the timezone the script or its host happens to run in.

That dependence was real until recently: `scheduled-scrape.yml` sets `TZ` only on the
commit step, so the scrape step runs on the runner's UTC clock. The script no longer relies
on the ambient zone for dates, but anything else added to that step should set `TZ` itself
rather than inherit it.

`consolidate.py` reassigns the `future` name mid-loop from a list to a generator. It
works, but reads oddly.
