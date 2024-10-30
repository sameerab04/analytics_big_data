# Word Frequency MapReduce Scripts

## Overview

These Python scripts implement a MapReduce pipeline to process word frequency data from Google Books. The goal is to calculate the average number of volumes per year for words containing specific substrings: "nu," "chi," and "haw." The scripts are designed to run in a Hadoop environment using Hadoop Streaming, allowing them to be used as custom mappers and reducers.

## File Descriptions

- **mapper_gram.py**: Processes each line from the dataset, filters for specific substrings within words, and outputs relevant data for further aggregation.
- **sorter.py**: Sorts the intermediate output from the mapper phase to prepare it for the reducer phase.
- **reducer_gram.py**: Aggregates and calculates the average volume for each `(year, substring)` key from the sorted output.

## Usage

These scripts are designed to be used in a Hadoop environment with Hadoop Streaming. Below is an example command to run the MapReduce job:

```bash
hadoop jar /path/to/hadoop-streaming.jar \
    -files mapper1.py,reducer1.py,sorter.py \
    -input /path/to/input_data \
    -output /path/to/output_data \
    -mapper mapper1.py \
    -reducer reducer1.py \
    -combiner sorter.py
```
