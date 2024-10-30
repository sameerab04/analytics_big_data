## Pseudocode for MapReduce Script

### Function: `mapper(input_file)`
- Initialize an empty list: `intermediate_data`
- Open `input_file` for reading.
- Read each line using CSV reader.
- For each `row` in the file:
  - Check if `row` has at least four columns.
  - Extract the artist's name (third column) and duration (fourth column).
  - Attempt to convert `duration` to float.
    - If successful, append `(artist_name, duration)` to `intermediate_data`.
    - If not, ignore the row (error in conversion).
- Return `intermediate_data`.

### Function: `shuffle_and_reduce(intermediate_data)`
- Initialize a dictionary: `shuffled` to collect lists of durations by artist.
- For each `(artist, duration)` pair in `intermediate_data`:
  - Append `duration` to the list in `shuffled[artist]`.
- Initialize a dictionary: `reduced_data` for final output.
- For each `artist` in `shuffled`:
  - Calculate the maximum duration from `shuffled[artist]`.
  - Assign the max duration to `reduced_data[artist]`.
- Return `reduced_data`.

### Main Execution
- If script is executed with incorrect arguments:
  - Display usage message and exit.
- Extract `input_dir` and `num_reducers` from command-line arguments.
- List all CSV files in `input_dir`.
- Determine the number of mappers as the length of the list of CSV files.
- Create a pool of workers (`Pool`) equal to the number of mappers.
- Map `mapper` function across all input files using the pool.
- Combine results from all mappers into a single list: `combined_intermediate_data`.
- Apply `shuffle_and_reduce` to `combined_intermediate_data`.
- Sort the results from the reduce phase by artist name.
- Open `final_output.txt` for writing.
- Write each artist and their maximum duration to the file.
