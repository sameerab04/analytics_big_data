import os
import csv
from multiprocessing import Pool
from collections import defaultdict
import sys

# Define the mapper function
def mapper(input_file):
    intermediate_data = []
    with open(input_file, 'r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            if len(row) >= 4:
                artist_name = row[2].strip()
                try:
                    duration = float(row[3].strip())
                    intermediate_data.append((artist_name, duration))
                except ValueError:
                    pass  # Ignore invalid duration values
    return intermediate_data

# Function to reduce a single artist's data
def reducer(artist_data):
    artist, durations = artist_data
    max_duration = max(durations)
    return artist, max_duration

# Function to shuffle and reduce data
def shuffle_and_reduce(intermediate_data, num_reducers):
    # Shuffle phase
    shuffled = defaultdict(list)
    for artist, duration in intermediate_data:
        shuffled[artist].append(duration)

    # Parallel Reduce phase
    with Pool(num_reducers) as pool:
        reduced_data = pool.map(reducer, shuffled.items())

    return dict(reduced_data)

# Entry point for script
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 songs.py <input_directory> <num_reducers>")
        sys.exit(1)

    input_dir = sys.argv[1]
    num_reducers = int(sys.argv[2])

    # Execute the MapReduce process
    input_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.csv')]
    num_mappers = len(input_files)

    # Map phase
    with Pool(num_mappers) as pool:
        intermediate_lists = pool.map(mapper, input_files)

    # Combine all intermediate data from mappers
    combined_intermediate_data = [item for sublist in intermediate_lists for item in sublist]

    # Shuffle and Reduce phase
    reduced_data = shuffle_and_reduce(combined_intermediate_data, num_reducers)

    # Write combined and sorted reduce phase outputs to a single file
    with open('output.txt', 'w') as f:
        for artist, max_duration in sorted(reduced_data.items()):
            f.write(f"{artist}\t{max_duration}\n")
