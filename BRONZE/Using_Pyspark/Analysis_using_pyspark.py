from pyspark.sql import SparkSession
import Config_pyspark
import os
from pyspark.sql.functions import count, avg, max, weekofyear, year, date_sub, current_date, col
from pyspark.sql import functions as F

if __name__ == '__main__':
    # Initialization of Google application
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Config.GOOGLE_APPLICATION_CREDENTIALS

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("ReadFromBigQuery") \
        .getOrCreate()

    # Read data from BigQuery
    df = spark.read \
        .format("bigquery") \
        .option("project", Config.Project_id) \
        .option("table", f"{Config.Project_id}.{Config_pyspark.Dataset}.{Config_pyspark.Table}") \
        .load()

    # Show DataFrame schema and data
    df.printSchema()
    df.show()


 # 1. Count the number of earthquakes by region

    df.groupBy('area').agg(count('*').alias('Number of Earthquakes')).show()

 # 2. Find the average magnitude by the region

    df.groupBy('area').agg(avg('mag').alias('Avg magnitude by region')).show()

 # 3. Find how many earthquakes happen on the same day.

    df.groupBy('insert_dt').agg(count('*').alias('Number of earthquakes on same day')).show()

 # 4. Find how many earthquakes happen on same day and in same region

    df.groupBy('area','insert_dt').agg(count('*').alias('Number of earthquakes on same day and region')).show()

 # 5. Find average earthquakes happen on the same day.

    # First, count the number of earthquakes for each day and store it in a temporary DataFrame
    daily_counts_df = df.groupBy('insert_dt').agg(count('*').alias('daily_count'))

    # Then, calculate the average of the daily counts
    daily_counts_df.agg(avg('daily_count').alias('Avg earthquakes on same day')).show()

 # 6. Find average earthquakes happen on same day and in same region

    # Group by 'insert_dt' and 'region' to count the number of earthquakes for each combination
    daily_region_counts_df = df.groupBy('insert_dt', 'area').agg(count('*').alias('daily_region_count'))

    # Calculate the average number of earthquakes for each day and region
    daily_region_counts_df.agg(avg('daily_region_count').alias('Avg earthquakes on same day and region')).show()

 # 7. Find the region name, which had the highest magnitude earthquake last week.

    # Filter the DataFrame for records from last week
    last_week_df = df.filter(
        (year(col("insert_dt")) == year(current_date())) &
        (weekofyear(col("insert_dt")) == weekofyear(date_sub(current_date(), 7)))
    )

    # Find the region with the highest magnitude
    max_magnitude_region_df = last_week_df.orderBy(col("mag").desc()).select("area", "mag").limit(1)

    # Show the result
    max_magnitude_region_df.show()

 # 8. Find the region name, which is having magnitudes higher than 5.

    # Filter the data for magnitudes higher than 5
    filtered_df = df.where(df['mag'] > 5)

    # Select the region names and remove duplicates (if any)
    region_names = filtered_df.select('area').distinct()

    # Show the result
    region_names.show()

 # 9. Find out the regions which are having the highest frequency and intensity of earthquakes.

    # Assuming the data is loaded into a DataFrame `df` with columns 'mag' and 'place'

    # Group by 'place' to count the number of earthquakes (frequency) per region
    frequency_df = df.groupBy('place').agg(F.count('mag').alias('earthquake_frequency'))

    # Sort by earthquake frequency in descending order
    frequency_df = frequency_df.orderBy('earthquake_frequency', ascending=False)

    # Show the regions with the highest frequency
    frequency_df.show()

    # Group by 'place' and calculate the sum of magnitudes (intensity) per region
    intensity_df = df.groupBy('place').agg(F.sum('mag').alias('total_intensity'))

    # Sort by total intensity in descending order
    intensity_df = intensity_df.orderBy('total_intensity', ascending=False)

    # Show the regions with the highest total intensity
    intensity_df.show()

    # Combine the two DataFrames to show both frequency and intensity for each region
    combined_df = frequency_df.join(intensity_df, on='place')

    # Show the combined results
    combined_df.show()