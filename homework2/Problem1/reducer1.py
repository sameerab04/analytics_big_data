#!/usr/bin/env python3
import sys

# Initialize a dictionary to hold volumes and counts
volume_data = {}

# Read each line from stdin
for line in sys.stdin:
    # Parse the key and volume from the value
    key, volume_str = line.strip().split("\t")
    volume = float(volume_str)

    # Accumulate volumes and counts for each key
    if key in volume_data:
        volume_data[key]["total"] += volume
        volume_data[key]["count"] += 1
    else:
        volume_data[key] = {"total": volume, "count": 1}

# Calculate and print the average volume for each key
for key, data in volume_data.items():
    average_volume = data["total"] / data["count"]
    print(f"{key},{average_volume}")
