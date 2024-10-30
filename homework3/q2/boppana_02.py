# Importing the libraries 
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.stat import Statistics
from pyspark.mllib.linalg import Vectors

from datetime import datetime
import numpy as np
import itertools
from scipy.stats import ttest_ind

# Initiating a Spark Session 
spark = SparkSession \
    .builder \
    .appName("Chicago_Crime2") \
    .getOrCreate()

# Initiating a SparkContext from the SparkSession
sc = spark.sparkContext

# Setting up log level 
sc.setLogLevel("ERROR")

# Loading the Data 
#crime_path = 'Crimes_-_2001_to_present.csv'
crime = sc.textFile("hdfs:///user/omo6093/Assignment3/crime/Crimes_-_2001_to_present.csv")

# Extracting the header
header = crime.first()

# Filtering out the header
crime_filtered = crime.filter(lambda x: x != header)

# Part 1 - Top 10 blocks in crime events in the last 3 years
# Function to parse the csv file
def parse_csv(line):
    fields = line.split(',')
    try:
        date = datetime.strptime(fields[2], '%m/%d/%Y %I:%M:%S %p')
        year = date.year
    except (ValueError, IndexError):
        year = None  
    return {
        'Date': fields[2],
        'Block': fields[3],
        'Year': year
    }

# Parse data and extract year
parsed_data = crime_filtered.map(parse_csv)

# Find the maximum year in the dataset
max_year = parsed_data.map(lambda record: record['Year']).max()

# Parse data, filter last three years, and extract blocks
recent_crimes = (parsed_data
    .filter(lambda record: record['Year'] is not None and (max_year - 3) <= record['Year'] <= max_year)
    .map(lambda record: (record['Block'], 1)))

# Reduce by key to get crime counts per block
block_counts = recent_crimes.reduceByKey(lambda a, b: a + b)

# Sort by the number of crimes in descending order
top_blocks = block_counts.sortBy(lambda pair: pair[1], ascending=False).take(10)

# Open a file to write the result
with open('boppana_02_p1_output.txt', 'w') as f:
    for block, count in top_blocks:
        f.write(f"Block: {block}, Number of Crimes: {count}\n")

# Part 2 - Two beats that are adjacent with the highest correlation in the number of crime events 
# Split each line by comma and parse into fields
parsed_crime = crime_filtered.map(lambda x: x.split(","))

# Filter for relevant columns and last 5 years
filtered_crime = parsed_crime.filter(lambda x: x[17].isdigit() and x[10].isdigit()) \
                              .filter(lambda x: int(x[17]) >= (max_year - 5)) \
                              .map(lambda x: (int(x[17]), int(x[10])))

# Group the data by year
grouped_data = filtered_crime.groupByKey()

# Calculate the count for each beat within each year
crime_counts_by_year = grouped_data.mapValues(lambda beats: {beat: sum(1 for _ in beats if _ == beat) for beat in set(beats)})

# Create a set of all unique beats
all_beats = crime_counts_by_year.flatMap(lambda x: x[1].keys()).distinct().collect()
all_beats_dict = {beat: index for index, beat in enumerate(all_beats)}
num_beats = len(all_beats)

# Count the number of crimes per beat per year and switch the order of beat and year
beatCountsPerYear = crime_counts_by_year.flatMap(lambda x: [((x[0], beat), count) for beat, count in x[1].items()])

# Create a full vector for each year
beatCountsPerYearFull = beatCountsPerYear.map(lambda x: (x[0][0], (all_beats_dict[x[0][1]], x[1]))).groupByKey()

def fill_vector(counts):
    vec = [0] * num_beats
    for beat_index, count in counts:
        vec[beat_index] = count
    return Vectors.dense(vec)

countsVectors = beatCountsPerYearFull.mapValues(fill_vector)

# Compute correlation matrix
correlation_matrix = Statistics.corr(countsVectors.values())

# Create a mapping of beats to indices
beat_to_index = sc.broadcast({beat: index for index, beat in enumerate(all_beats)})

# Create a list to store beat pairs and their correlation
beat_pairs_correlation = []

# Iterate over the correlation matrix
for i in range(correlation_matrix.shape[0]):
    for j in range(i+1, correlation_matrix.shape[1]): # Only pairs (i, j) with j > i
        beat_pairs_correlation.append((all_beats[i], all_beats[j], correlation_matrix[i][j]))

# Sort the list by correlation in descending order
beat_pairs_correlation.sort(key=lambda x: x[2], reverse=True)

# Take the top 10 pairs
top_10_pairs = beat_pairs_correlation[:10]

# Write the top 10 pairs to a file
with open("boppana_02_p2_output.txt", "w") as file:
    file.write("Top 10 pairs of beats with the highest correlation:\n")
    for pair in top_10_pairs:
        file.write(f"Beats: {pair[0]} and {pair[1]}, Correlation: {pair[2]}\n")


# Part 3 - Difference in crime levels for Mayors Daly and Emanuel 
# Define the time periods for each mayor's term
daley_start = datetime(1989, 4, 24)
daley_end = datetime(2011, 5, 16)
emanuel_start = datetime(2011, 5, 16)
emanuel_end = datetime(2019, 5, 20)

# Function to parse the crime data
def parse_crime(line):
    fields = line.split(',')
    date = datetime.strptime(fields[2], '%m/%d/%Y %I:%M:%S %p')
    community_area = fields[13]
    return (date, community_area)

# Parse the crime data and filter for each mayor's term at the community area level
daley_crimes = crime_filtered.map(parse_crime).filter(lambda x: daley_start <= x[0] < daley_end)
emanuel_crimes = crime_filtered.map(parse_crime).filter(lambda x: emanuel_start <= x[0] < emanuel_end)

# Analyze difference at the community area level
daley_comm_counts = daley_crimes.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
emanuel_comm_counts = emanuel_crimes.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

# Combine the crime counts for both mayors
combined_comm_counts = daley_comm_counts.fullOuterJoin(emanuel_comm_counts)

# Extract crime counts for each mayor and handle missing values
daley_comm_list = combined_comm_counts.map(lambda x: x[1][0] if x[1][0] is not None else 0).collect()
emanuel_comm_list = combined_comm_counts.map(lambda x: x[1][1] if x[1][1] is not None else 0).collect()

# Perform t-test to compare crime counts between the two mayors
t_test_result = ttest_ind(daley_comm_list, emanuel_comm_list)
p_value = t_test_result.pvalue

# Compute mean and standard deviation of crime counts for each mayor's term
daley_comm_mean = sum(daley_comm_list) / len(daley_comm_list)
daley_comm_sd = sc.parallelize(daley_comm_list).stdev()
emanuel_comm_mean = sum(emanuel_comm_list) / len(emanuel_comm_list)
emanuel_comm_sd = sc.parallelize(emanuel_comm_list).stdev()

# Save the results to a file
with open("boppana_2_3.txt", "w") as file:
    file.write("Community Area Level:\n")
    file.write(f"p-value: {p_value}\n")
    if p_value < 0.05:
        file.write("There is a significant difference in crime counts between Mayors Daley and Emanuel at the community area level.\n")
    else:
        file.write("There is no significant difference in crime counts between Mayors Daley and Emanuel at the community area level.\n")
    file.write(f"Daley's Term - Mean: {daley_comm_mean}, Standard Deviation: {daley_comm_sd}\n")
    file.write(f"Emanuel's Term - Mean: {emanuel_comm_mean}, Standard Deviation: {emanuel_comm_sd}\n")

# Stop SparkContext
sc.stop()