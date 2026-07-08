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

if __name__ == "__main__":

    consolidated = [] # [0] is oldest [-1] is youngest
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
                # check we don't already have a duplicate in the last N consolidated
                # (this can happen if the schedule is updated a bit late)
                if consolidated and serialise(movement) == serialise(consolidated[-1]):
                    continue
                consolidated.append(movement)
            else: # chronological, so can short circuit
                break

        # whole current schedule is new future
        future = (deserialise(line) for line in get_lines_in_file(file_path))
    
    # Update
    print(f"Generated consolidated historical schedule with {len(consolidated)} movements")
    print(f"Most newest future schedule: {files[-1]}")

    # store what we have
    write_to_jsonl(consolidated, "historical.jsonl")
    write_to_jsonl(future, "newest.jsonl")
    print("Wrote historical.jsonl and newest.jsonl to file")

