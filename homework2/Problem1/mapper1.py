#!/usr/bin/env python3
import sys

# Define substrings to look for
substring_array = ["nu", "chi", "haw"]

# Read each line from stdin
for line in sys.stdin:
    # Split the line based on whitespace
    line_split = line.strip().split()
    if len(line_split) < 4:
        continue

    word, year, occurrences, volume = line_split[0], line_split[1], line_split[2], line_split[3]

    # Check if the year is valid
    try:
        year_int = int(year)
        if not (0 < year_int <= 2024):
            continue
    except ValueError:
        continue

    # Emit the volume once for each substring present in the word
    for substring in substring_array:
        if substring in word:
            print(f"{year},{substring}\t{volume}")
            break
