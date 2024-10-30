import os
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, weekofyear, year, lit, count, when, countDistinct,last,  sum as sql_sum, lpad, max as spark_max
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor, LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import Window
from pyspark.sql.functions import lag
from fairlearn.metrics import MetricFrame
from sklearn.metrics import mean_squared_error
from pyspark.sql.functions import year, weekofyear, date_add, lit
import pandas as pd
from pyspark.sql import Row



def load_data(spark, crime_path, iucr_path, stock_path, demographic_path, filter_violent=False, violent_crimes=None):
    """Load and praocess datasets."""
    crime_df = spark.read.csv(crime_path, header=True)
    crime_df = crime_df.withColumn("Date", to_date(col("Date"), 'MM/dd/yyyy hh:mm:ss a'))

    if filter_violent and violent_crimes:
        crime_df = crime_df.filter(col("Primary Type").isin(violent_crimes))
    iucr_df = spark.read.csv(iucr_path, header=True)
    iucr_df = iucr_df.withColumnRenamed("IUCR", "IUCR_other")

    stock_df = spark.read.csv(stock_path, header=True)
    stock_df = stock_df.withColumn("Date", to_date(col("Date"), 'yyyy-MM-dd'))
    stock_df = stock_df.withColumn("Close", col("Close").cast("float"))
    stock_df = stock_df.withColumn("week_of_year", weekofyear(col("Date")))
    stock_df = stock_df.withColumn("year", year(col("Date")))

    demographic_df = spark.read.csv(demographic_path, header=True)
    demographic_df = demographic_df.withColumn("beat_padded", lpad(col("beat"), 4, '0')).drop("beat")

    # Convert all demographic numerical columns to float type
    for col_name in demographic_df.columns:
        if col_name != 'beat_padded':
            demographic_df = demographic_df.withColumn(col_name, col(col_name).cast('float'))

    return crime_df, iucr_df, stock_df, demographic_df


def compute_weekly_stock_metrics(stock_df):
    """Compute average weekly closing price and weekly closing change."""
    weekly_stock_df = stock_df.groupBy("year", "week_of_year").avg("Close")
    weekly_stock_df = weekly_stock_df.withColumnRenamed("avg(Close)", "avg_weekly_close")
    weekly_stock_df = weekly_stock_df.withColumn('avg_weekly_close_lag_1', lag('avg_weekly_close', 1).over(Window.orderBy('year', 'week_of_year')))
    weekly_stock_df = weekly_stock_df.withColumn('weekly_close_change', (col('avg_weekly_close') - col('avg_weekly_close_lag_1')) / col('avg_weekly_close_lag_1') * 100)
    weekly_stock_df = weekly_stock_df.na.fill(0)

    return weekly_stock_df


def merge_and_feature_engineering(crime_df, iucr_df, stock_df, demographic_df, violent_crimes, non_violent_crimes):
    """Merge datasets and perform feature engineering."""
    joined_df = crime_df.join(iucr_df, crime_df.IUCR == iucr_df.IUCR_other, 'inner')
    joined_df = joined_df.withColumn("week_of_year", weekofyear(col("Date")))
    joined_df = joined_df.withColumn("year", year(col("Date")))
    joined_df = joined_df.join(stock_df, ['year', 'week_of_year'], 'inner')

    indexer = StringIndexer(inputCol="Beat", outputCol="BeatIndex")
    joined_df = indexer.fit(joined_df).transform(joined_df)

    joined_df = joined_df.withColumn("violent_crime", when(col("Primary Type").isin(violent_crimes), 1).otherwise(0))
    joined_df = joined_df.withColumn("non_violent_crime", when(col("Primary Type").isin(non_violent_crimes), 1).otherwise(0))
    joined_df = joined_df.withColumn("Arrest", when(col("Arrest") == True, 1).otherwise(0))
    joined_df = joined_df.withColumn("Domestic", when(col("Domestic") == True, 1).otherwise(0))

    aggregated_df = joined_df.groupBy("Beat", "BeatIndex", "year", "week_of_year", "avg_weekly_close", "weekly_close_change") \
        .agg(sql_sum(when(col("violent_crime") == lit(1), 1)).alias("total_violent_crimes"),
             sql_sum(when(col("non_violent_crime") == lit(1), 1)).alias("total_non_violent_crimes"),
             sql_sum(col("Arrest")).alias("total_arrests"),
             sql_sum(col("Domestic")).alias("total_domestic_crimes"),
             countDistinct("District").alias("num_districts"),
             countDistinct("Ward").alias("num_wards"),
             countDistinct("Community Area").alias("num_community_areas"),
             countDistinct("Location Description").alias("num_location_descriptions"))

    aggregated_df = aggregated_df.withColumn("total_crimes", col("total_violent_crimes") + col("total_non_violent_crimes"))

    window_spec = Window.partitionBy('Beat').orderBy('year', 'week_of_year')
    for num_weeks_lag in [1, 2, 3, 4]:
        aggregated_df = aggregated_df.withColumn(f'total_crimes_lag_{num_weeks_lag}', lag('total_crimes', num_weeks_lag).over(window_spec))
        aggregated_df = aggregated_df.withColumn(f'total_arrests_lag_{num_weeks_lag}', lag('total_arrests', num_weeks_lag).over(window_spec))
        aggregated_df = aggregated_df.withColumn(f'total_domestic_crimes_lag_{num_weeks_lag}', lag('total_domestic_crimes', num_weeks_lag).over(window_spec))

    aggregated_df = aggregated_df.join(demographic_df, aggregated_df.Beat == demographic_df.beat_padded, 'inner')

    aggregated_df = aggregated_df.na.fill(0)

    return aggregated_df


def train_and_evaluate_models(training_data, test_data, training_columns):
    """Trains and evaluates regression models."""
    assembler = VectorAssembler(inputCols=training_columns, outputCol="features")
    rf = RandomForestRegressor(labelCol="total_crimes", featuresCol="features")
    gbt = GBTRegressor(labelCol="total_crimes", featuresCol="features")
    lr = LinearRegression(labelCol="total_crimes", featuresCol="features")

    pipeline_rf = Pipeline(stages=[assembler, rf])
    pipeline_gbt = Pipeline(stages=[assembler, gbt])
    pipeline_lr = Pipeline(stages=[assembler, lr])

    training_data.persist()
    test_data.persist()

    model_results = {}
    models = {}
    for name, pipeline in [('RF', pipeline_rf), ('GBT', pipeline_gbt), ('LR', pipeline_lr)]:
        model = pipeline.fit(training_data)
        predictions = model.transform(test_data)

        evaluators = {
            "rmse": RegressionEvaluator(labelCol="total_crimes", predictionCol="prediction", metricName="rmse"),
            "r2": RegressionEvaluator(labelCol="total_crimes", predictionCol="prediction", metricName="r2"),
            "mae": RegressionEvaluator(labelCol="total_crimes", predictionCol="prediction", metricName="mae")
        }
        
        results = {metric_name: round(evaluator.evaluate(predictions), 2) for metric_name, evaluator in evaluators.items()}
        model_results[name] = results
        models[name] = model
        
    return model_results, models, predictions


def evaluate_bias(predictions, sensitive_feature, name):
    """Evaluate bias metrics."""
    if not os.path.exists("./bias"):
        os.makedirs("./bias")

    pandas_test_data = predictions.toPandas()
    sensitive_features_binned = pd.cut(pandas_test_data[sensitive_feature], bins=5, labels=False, right=False)

    metric_frame = MetricFrame(metrics=mean_squared_error, y_true=pandas_test_data["total_crimes"], y_pred=pandas_test_data["prediction"], sensitive_features=sensitive_features_binned)
    sorted_group_metrics = metric_frame.by_group.sort_index()

    with open(f"./bias/boppana_{name}_bias_metrics.txt", 'w') as bias_f:
        bias_f.write(f"Overall fairness metrics for {name}:\n")
        bias_f.write(str(metric_frame.overall))
        bias_f.write("\n\n")
        bias_f.write("Group metrics:\n")
        bias_f.write(str(sorted_group_metrics))
        bias_f.write("\n\n")

    income_values = sorted_group_metrics.index
    mse_values = sorted_group_metrics.values

    plt.bar(income_values, mse_values)
    plt.xlabel('Income')
    plt.ylabel('Mean Squared Error')
    plt.title(f'Bias Metrics for {name}')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'./bias/boppana_{name}_hist.png')
    plt.close()


def save_model_results(model_results, file_path):
    """Save model results to a text file."""
    with open(file_path, 'w') as model_output_file:
        for model_name, results in model_results.items():
            model_output_file.write(f"Model: {model_name}\n")
            for metric_name, metric_value in results.items():
                model_output_file.write(f"{metric_name}: {metric_value}\n")
            model_output_file.write("\n")


def generate_synthetic_test_data(data, latest_year, latest_week_number):
    # Calculate the corresponding week of the previous year
    previous_year = latest_year - 1
    previous_week_number = latest_week_number
    
    # If the current week is 1, consider the last week of the previous year
    if previous_week_number == 1:
        previous_week_number = 52
    else:
        previous_week_number -= 1
    
    # Filter data for the corresponding week of the previous year
    synthetic_test_data = data.filter((col("year") == previous_year) & (col("week_of_year") == previous_week_number))
    
    # Adjust the year and week_of_year columns to represent the next week
    synthetic_test_data = synthetic_test_data.withColumn(
        "week_of_year",
        when(col("week_of_year") == 52, 1).otherwise(col("week_of_year") + 1)
    ).withColumn(
        "year",
        when(col("week_of_year") == 52, col("year") + 1).otherwise(col("year"))
    )
    
    return synthetic_test_data

def predict_next_week(data, assembler, model, output_file):
    # Calculate the next week and year for prediction
    latest_week_info = data.agg({"year": "max", "week_of_year": "max"}).collect()[0]
    latest_year = latest_week_info["max(year)"]
    latest_week_number = latest_week_info["max(week_of_year)"]
    
    # Generate synthetic test data for the next week
    synthetic_test_data = generate_synthetic_test_data(data, latest_year, latest_week_number)
    print("Syntehtic data")

    # Prepare features for the next week
    next_week_features = assembler.transform(synthetic_test_data)

    # Generate predictions
    next_week_features = next_week_features.drop("features")
    print("Next week features")
    print(next_week_features)
    predictions = model.transform(next_week_features).withColumnRenamed("prediction", "predicted_features")
    predictions = predictions.select("Beat", "predicted_features")

    # Save predictions to a CSV file
    predictions.toPandas().to_csv(output_file, index=False)

    print(f"Predictions for the next week saved to: {output_file}")

    return predictions


if __name__ == "__main__":
    # Start Spark session
    spark = SparkSession.builder.appName("ChicagoCrime3").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Define paths
    crime_path = "hdfs:///user/omo6093/Assignment3/crime/Crimes_-_2001_to_present.csv"
    iucr_path = "hdfs:///user/omo6093/Assignment3/crime/IUCR.csv"
    stock_path = "hdfs:///user/omo6093/Assignment3/crime/stocks.csv"
    demographic_path = "hdfs:///user/omo6093/Assignment3/crime/demographic_data.csv"

    # Load all crime data
    print("Loading All Crime Data")
    crime_df, iucr_df, stock_df, demographic_df = load_data(spark, crime_path, iucr_path, stock_path, demographic_path)

    # Define Violent and Non-Violent Crimes
    VIOLENT_CRIMES = ["OFFENSE INVOLVING CHILDREN", "PUBLIC PEACE VIOLATION", "ARSON", "ASSAULT", "BATTERY", "ROBBERY",
                      "HUMAN TRAFFICKING", "SEX OFFENSE", "CRIMINAL DAMAGE", "KIDNAPPING",
                      "INTERFERENCE WITH PUBLIC OFFICER"]

    NON_VIOLENT_CRIMES = ["OBSCENITY", "OTHER OFFENSE", "GAMBLING", "CRIMINAL TRESPASS", "LIQUOR LAW VIOLATION",
                          "PUBLIC INDECENCY", "INTIMIDATION", "PROSTITUTION", "DECEPTIVE PRACTICE",
                          "CONCEALED CARRY LICENSE VIOLATION", "NARCOTICS", "NON-CRIMINAL", "WEAPONS VIOLATION",
                          "OTHER NARCOTIC VIOLATION"]
    
    VIOLENT_CRIME_IUCR_CODES = [
            2090.0, 1661.0, 1500.0, 460.0, 486.0, 
            488.0, 497.0, 486.0, 488.0, 460.0, 
            31.0, 1150.0, 1151.0, 261.0, 1153.0, 
            1310.0, 100.0, 1520.0, 1611.0
        ]
    
    TRAINING_COLUMNS = [
        'BeatIndex', 'year', 'week_of_year', 'avg_weekly_close', 'weekly_close_change', 
        'num_white', 'num_hispanic', 'num_black', 'num_asian', 'num_mixed', 'num_other', 
        'bachelors', 'high_school', 'no_high_school', 'population', 'med_income', 
        'num_districts', 'num_wards', 'num_community_areas', 'num_location_descriptions', 
        'total_crimes_lag_1', 'total_arrests_lag_1', 'total_domestic_crimes_lag_1', 
        'total_crimes_lag_2', 'total_arrests_lag_2', 'total_domestic_crimes_lag_2', 
        'total_crimes_lag_3', 'total_arrests_lag_3', 'total_domestic_crimes_lag_3', 
        'total_crimes_lag_4', 'total_arrests_lag_4', 'total_domestic_crimes_lag_4'
    ]
    
    print("Processing All Crime Data")
    weekly_stock_df = compute_weekly_stock_metrics(stock_df)

    aggregated_df_all = merge_and_feature_engineering(crime_df, iucr_df, weekly_stock_df, demographic_df, VIOLENT_CRIMES, NON_VIOLENT_CRIMES)


    print("Splitting All Crime Data")
    training_data_all, test_data_all = aggregated_df_all.randomSplit([0.8, 0.2], seed=1234)

    print("Training and Evaluating All Crime Data")
    model_results_all, models_all, predictions_all = train_and_evaluate_models(training_data_all, test_data_all, TRAINING_COLUMNS)

    print("Saving All Crime Model Results")
    save_model_results(model_results_all, "boppana_03_allcrime_output.txt")

    print("Evaluating Bias for All Crime Data")
    evaluate_bias(predictions_all, sensitive_feature="med_income", name="all_crime")

    crime_df = crime_df.withColumn("week_of_year", weekofyear("Date"))

    # Predict next week's crimes
    print("Predicting Next Week's Crimes")

    # Create VectorAssembler
    assembler = VectorAssembler(inputCols=TRAINING_COLUMNS, outputCol="features")

    # Predict next week's crimes
    predict_next_week(aggregated_df_all, assembler, models_all["RF"], "boppana_03_predictions.csv")

    print("Evaluating only on Violent Test Data")
    # Filter the test data to include only violent crimes
    test_data_violent_only = test_data_all.filter(col("total_violent_crimes") > 0)

    # Initialize a dictionary to store model results
    model_results = {}

    # Evaluate each model
    for model_name, model in models_all.items():
        # Make predictions
        predictions = model.transform(test_data_violent_only)

        # RMSE
        evaluator = RegressionEvaluator(labelCol="total_crimes", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)

        # R-squared
        evaluator = RegressionEvaluator(labelCol="total_crimes", predictionCol="prediction", metricName="r2")
        r2 = evaluator.evaluate(predictions)

        # MAE
        evaluator = RegressionEvaluator(labelCol="total_crimes", predictionCol="prediction", metricName="mae")
        mae = evaluator.evaluate(predictions)

        # Store results in the model_results dictionary
        model_results[model_name] = {"RMSE": rmse, "R-squared": r2, "MAE": mae}

    # Write metrics for all models to a file
    with open("boppana_03_violent_output.txt", 'w') as file:
        for model_name, metrics in model_results.items():
            file.write(f"Model: {model_name}\n")
            file.write(f"RMSE: {metrics['RMSE']}\n")
            file.write(f"R-squared: {metrics['R-squared']}\n")
            file.write(f"MAE: {metrics['MAE']}\n")
            file.write("\n")

    # Stop Spark session
    spark.stop()


