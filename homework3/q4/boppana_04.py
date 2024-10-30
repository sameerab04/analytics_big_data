# Import the libraries  
from pyspark.sql import SparkSession
from pyspark.sql.functions import max,col, weekofyear, year, to_timestamp, dayofweek, month, hour, avg, lit, last
from pyspark.sql.types import IntegerType, FloatType, BooleanType, StringType, TimestampType, TimestampType
from pyspark.sql.functions import count, sum, col, format_number

import matplotlib.pyplot as plt
import os

# Initiating a Spark Session 
spark = SparkSession.builder.appName("ChicagoCrime4").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Loading the Data 

crime = spark.read.csv("hdfs:///user/omo6093/Assignment3/crime/Crimes_-_2001_to_present.csv", header = True)

# Preparing and cleaning the data 
# Preparing and cleaning the data
crime = crime.withColumn("Date", to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"))
crime = crime.withColumn("Arrest", (col("Arrest") == "true").cast(BooleanType()))
crime = crime.withColumn("Arrest", col("Arrest").cast("boolean").cast("integer"))
crime = crime.select("ID", "Date", "Arrest", "Primary Type")

# Extracting Time Components
crime = crime.withColumn("Hour", hour("Date"))
crime = crime.withColumn("DayOfWeek", dayofweek("Date"))  # Sunday=1, Saturday=7
crime = crime.withColumn("Month", month("Date"))

# Descriptive Statistics 
# Aggregations for each time component with percentage of arrests
stats_by_hour = crime.groupBy("Hour").agg(
    count("ID").alias("Total Crimes"),
    sum("Arrest").alias("Total Arrests"),
    (sum("Arrest") / count("ID") * 100).alias("Percentage of Arrests")
).orderBy("Hour")

stats_by_day = crime.groupBy("DayOfWeek").agg(
    count("ID").alias("Total Crimes"),
    sum("Arrest").alias("Total Arrests"),
    (sum("Arrest") / count("ID") * 100).alias("Percentage of Arrests")
).orderBy("DayOfWeek")

stats_by_month = crime.groupBy("Month").agg(
    count("ID").alias("Total Crimes"),
    sum("Arrest").alias("Total Arrests"),
    (sum("Arrest") / count("ID") * 100).alias("Percentage of Arrests")
).orderBy("Month")

# Plotting the Trends
def plot_and_save_stats(df, title, xlabel, file_path):
    df_pd = df.toPandas()
    fig, ax1 = plt.subplots()

    # Bar plot for total crimes
    ax1.set_xlabel(xlabel)
    ax1.set_ylabel('Total Crimes', color='tab:blue')
    ax1.bar(df_pd[xlabel], df_pd['Total Crimes'], color='tab:blue')
    ax1.tick_params(axis='y', labelcolor='tab:blue')

    # Line plot for total arrests
    ax2 = ax1.twinx()
    ax2.set_ylabel('Total Arrests', color='tab:red')
    ax2.plot(df_pd[xlabel], df_pd['Total Arrests'], color='tab:red', marker='o')
    ax2.tick_params(axis='y', labelcolor='tab:red')

    plt.title(title)
    #plt.show()

    # Save the figure
    fig.savefig(file_path)
    plt.close(fig)


# Saving the graphs 
plot_and_save_stats(stats_by_hour, 'Crime Statistics by Hour of Day', 'Hour', 'boppana_04_hourly.png')
plot_and_save_stats(stats_by_day, 'Crime Statistics by Day of Week', 'DayOfWeek', 'boppana_04_daily.png')
plot_and_save_stats(stats_by_month, 'Crime Statistics by Month', 'Month', 'boppana_04_monthly.png')
