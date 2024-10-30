from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
import matplotlib.pyplot as plt

def load_data(spark, file_path):
    """
    Load data from the specified file path into a Spark DataFrame.
    
    Parameters:
        spark (SparkSession): Spark session object.
        file_path (str): Path to the input data file.
    
    Returns:
        DataFrame: Spark DataFrame containing the loaded data.
    """
    # Load data into DataFrame
    df = spark.read.option("header", "true").csv(file_path)
    df = df.withColumn("Date", to_date(col("Date"), 'MM/dd/yyyy hh:mm:ss a'))

    return df

def generate_monthly_histogram(df, spark):
    """
    Generate a histogram of average crime events by month using SparkSQL.
    
    Parameters:
        df (DataFrame): Spark DataFrame containing crime data.
        output_file (str): Path to the output CSV file.
    """
    # Register DataFrame as a temporary view
    df.createOrReplaceTempView("crime_data")
    
    # Perform SQL query to calculate average crime events by month
    monthly_avg_crime = spark.sql("""
    SELECT Month, ROUND(AVG(Crime_Count), 2) as Average_Crime_Events
    FROM (
        SELECT YEAR(Date) as Year, MONTH(Date) as Month, COUNT(*) as Crime_Count
        FROM crime_data
        GROUP BY YEAR(Date), MONTH(Date)
    ) as inner_table
    GROUP BY Month
    ORDER BY Month
""")
    
    # Collect the result as a list of Row
    crime_count = monthly_avg_crime.collect()
    
    # Open a file to write the result
    with open('boppana_01_output.txt', 'w') as f:
        for row in crime_count:
            f.write(f"Month: {row['Month']}, Average_Crime_Events:{row['Average_Crime_Events']}\n")
    
    # Create a bar plot of the average crime events by month directly from the collected list
    months = [row['Month'] for row in crime_count]
    avg_crime_events = [row['Average_Crime_Events'] for row in crime_count]

    plt.bar(months, avg_crime_events)
    plt.xlabel('Month')
    plt.ylabel('Average Number of Crime Events')
    plt.title('Histogram of average crime events by month')
    plt.savefig('boppana_01_hist.png')

def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("ChicagoCrimeAnalysis").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    
    # Path to the input data file
    file_path = "hdfs:///user/omo6093/Assignment3/crime/Crimes_-_2001_to_present.csv"
    
    # Load data into DataFrame
    crime_data = load_data(spark, file_path)
    
    # Generate histogram of average crime events by month and save to output file
    generate_monthly_histogram(crime_data, spark)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
