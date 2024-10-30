from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, date_format, avg, sum, last, abs
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GeneralizedLinearRegression
import pandas as pd
import boto3

def initialize_spark():
    return SparkSession.builder \
        .appName("TradingProfitPrediction") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def load_data_from_s3(spark, bucket_name, file_key):
    return spark.read.csv(f's3a://{bucket_name}/{file_key}', header=True, inferSchema=True)

def convert_columns(data):
    for column in data.columns:
        if column != "time_stamp":
            data = data.withColumn(column, col(column).cast(FloatType() if column.startswith("val") or column == "profit" else IntegerType()))
    return data

def add_bar_num_group_column(data):
    return data.withColumn("bar_num_group_10", ((col("bar_num") - 1) / 10).cast(IntegerType()) * 10)

def calculate_average_profit(data):
    grouped_data = data.filter(col('bar_num') % 10 == 0).groupBy('trade_id', 'bar_num_group_10').agg(avg('profit').alias('avg_profit'))
    grouped_data = grouped_data.withColumn('bar_num_group_10', col('bar_num_group_10') + 10)
    return data.join(grouped_data, on=['trade_id', 'bar_num_group_10'], how='left').orderBy("trade_id", "bar_num")

def forward_fill_average_profit(data):
    window_spec = Window.partitionBy('trade_id').orderBy('bar_num')
    return data.withColumn('avg_profit', last('avg_profit', ignorenulls=True).over(window_spec))

def create_lagged_features(data):
    window_spec_lag = Window.partitionBy("trade_id").orderBy("bar_num")
    for column in data.columns:
        if column.startswith("val"):
            for lag_val in range(1, 4):
                data = data.withColumn(f"{column}_lag{lag_val}", lag(col(column), lag_val).over(window_spec_lag))
    return data

def filter_null_avg_profit(data):
    return data.filter(~col("avg_profit").isNull())

def create_additional_features(data):
    window_spec = Window.partitionBy('trade_id').orderBy('bar_num')
    data = data.withColumn('rolling_avg_profit', avg('profit').over(window_spec.rowsBetween(-3, 0)))
    data = data.withColumn('cumulative_profit', sum('profit').over(window_spec))
    return data

def extract_month(data):
    return data.withColumn('month', date_format('time_stamp', 'yyyy-MM'))

def get_unique_months(data):
    return sorted([row['month'] for row in data.select("month").distinct().collect()])

def define_input_features(data):
    features_list = [col for col in data.columns if col not in {"time_stamp", "bar_num", "direction", "trade_id", "id", "month", "profit", "avg_profit"}]
    return VectorAssembler(inputCols=features_list, outputCol='features')

def train_model(data, assembler):
    glr = GeneralizedLinearRegression(labelCol='profit', featuresCol='features')
    return glr.fit(data)

def evaluate_predictions(predictions):
    return predictions.select(avg(abs((col('profit') - col('prediction')) / col('profit'))).alias('mape')).first()['mape']

def main():
    # Initialize Spark session
    spark = initialize_spark()

    # Load data from S3
    bucket_name = 'homework-trading'
    file_key = 'full_data.csv'
    data = load_data_from_s3(spark, bucket_name, file_key)

    # Data preprocessing
    data = convert_columns(data)
    data = add_bar_num_group_column(data)
    data = calculate_average_profit(data)
    data = forward_fill_average_profit(data)
    data = create_lagged_features(data)
    data = filter_null_avg_profit(data)
    data = create_additional_features(data)
    data = extract_month(data)

    # Get unique months
    unique_months = get_unique_months(data)

    # Define input features
    assembler = define_input_features(data)

    # Walk-forward validation
    results = []
    for start in range(0, len(unique_months) - 7, 6):
        if start + 7 >= len(unique_months):
            break

        train_months = unique_months[start:start + 6]
        test_month = unique_months[start + 6]

        training_set = data.filter(data.month.isin(train_months))
        testing_set = data.filter(data.month == test_month)

        # Transform data using assembler
        training_set = assembler.transform(training_set)
        testing_set = assembler.transform(testing_set)

        # Train the model
        model = train_model(training_set, assembler)

        # Predict on the test set
        predictions = model.transform(testing_set)

        # Evaluate predictions
        mape = evaluate_predictions(predictions)

        # Store results
        results.append((train_months[0], train_months[-1], test_month, mape))

    # Convert results to DataFrame
    results_df = pd.DataFrame(results, columns=["Train_Start_Month", "Train_End_Month", "Test_Month", "MAPE"])
    average_mape = round(results_df["MAPE"].mean(),2)
    max_mape = round(results_df["MAPE"].max(),2)
    min_mape = round(results_df["MAPE"].min(),2)

    # Save summary as a text file
    summary_text = f"Mean MAPE = {average_mape}\n"
    summary_text += f"Max MAPE = {max_mape}\n"
    summary_text += f"Min MAPE = {min_mape}\n"

    for _, row in results_df.iterrows():
        summary_text += f"Training from {row['Train_Start_Month']} to {row['Train_End_Month']}, Testing month: {row['Test_Month']}\n"
        summary_text += f"MAPE: {row['MAPE']}\n"
        summary_text += "----------------------------------\n"

    # Use boto3 to upload summary text to S3
    s3_client = boto3.client('s3')
    result_bucket = "omo6093-mlds431-hw4"
    result_location = "Exercise3.txt"
    s3_client.put_object(Body=summary_text, Bucket=result_bucket, Key=result_location)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
