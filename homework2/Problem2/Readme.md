
# MapReduce Script for Maximum Song Duration by Artist

## Overview

This Python script implements a MapReduce program that calculates the maximum song duration for each artist in a large dataset. The dataset is split into multiple CSV files, and the script uses Python's `multiprocessing` library to parallelize both the mapping and reducing phases for efficiency.

## Pseudocode

### `mapper(input_file)`

1. Initialize an empty list: `intermediate_data`.
2. Open `input_file` for reading.
3. Read each line using CSV reader.
4. For each row in the file:
   - Check if the row has at least four columns.
   - Extract the artist's name (third column) and duration (fourth column).
   - Attempt to convert duration to float.
   - If successful, append `(artist_name, duration)` to `intermediate_data`.
   - If unsuccessful (error in conversion), ignore the row.
5. Return `intermediate_data`.

### `shuffle_and_reduce(intermediate_data, num_reducers)`

1. Initialize a dictionary: `shuffled` to collect lists of durations by artist.
2. For each `(artist, duration)` pair in `intermediate_data`:
   - Append `duration` to the list in `shuffled[artist]`.
3. Initialize a dictionary: `reduced_data` for the final output.
4. For each artist in `shuffled`:
   - Calculate the maximum duration from `shuffled[artist]`.
   - Assign the max duration to `reduced_data[artist]`.
5. Return `reduced_data`.

### Main Execution

1. If the script is executed with incorrect arguments:
   - Display a usage message and exit.
2. Extract `input_dir` and `num_reducers` from the command-line arguments.
3. List all CSV files in `input_dir`.
4. Determine the number of mappers as the length of the list of CSV files.
5. Create a pool of workers (`Pool`) equal to the number of mappers.
6. Map the `mapper` function across all input files using the pool.
7. Combine results from all mappers into a single list: `combined_intermediate_data`.
8. Apply `shuffle_and_reduce` to `combined_intermediate_data`.
9. Sort the results from the reduce phase by artist name.
10. Open `output.txt` for writing.
11. Write each artist and their maximum duration to the file.


- Ensure that `output.txt` does not already exist, as it will be overwritten.
- The program is designed to handle errors in the duration column, where non-numeric values are ignored.
- Sorting of results is based on the artist name for a consistent output order.
