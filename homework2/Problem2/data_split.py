import csv
import os

# Function to split the data into smaller chunks
def split_data(file_path, num_splits, output_dir):
    # Determine the number of lines per split
    with open(file_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for line in f)
    lines_per_split = total_lines // num_splits

    # Create output directory if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Read the input file and write out the splits
    with open(file_path, 'r', encoding='utf-8') as f:
        for i in range(num_splits):
            with open(f"{output_dir}/data_split_{i}.csv", 'w', encoding='utf-8') as split_file:
                writer = csv.writer(split_file)
                # Write a portion of lines to each split file
                for line in (f.readline() for _ in range(lines_per_split + (1 if i < total_lines % num_splits else 0))):
                    writer.writerow(line.strip().split(','))

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python split_data.py <file_path> <num_splits> <output_dir>")
        sys.exit(1)

    file_path = sys.argv[1]
    num_splits = int(sys.argv[2])
    output_dir = sys.argv[3]

    split_data(file_path, num_splits, output_dir)
