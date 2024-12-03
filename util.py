import logging

################################################################################################
# Name : utils.py
# Description : Contains all common codes throughout the Earthquake ingestion project
# Date : 2024 - 10 - 23
# Version : 3.3.2
# Modified date : 2024 - 10 - 23
# Modification Description : we are going to use this utils files for the generic code
################################################################################################


from google.cloud import storage
import requests
from datetime import datetime
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, from_unixtime, regexp_extract, current_timestamp
from google.cloud import storage
import json
import logging
# Define the schema based on the structure of the JSON data
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, LongType, IntegerType


class Utils:

    def create_bucket(self,storage_client, bucket_name):
        """
        Creates a new bucket in Google Cloud Storage with the given name and standard storage class.
        Args:
            storage_client: A google.cloud.storage.Client instance.
            bucket_name (str): The name of the bucket to create.

        Returns:
            The created bucket object.
        """
        try:
            # Create a new bucket object
            bucket = storage_client.bucket(bucket_name)

            # Setting storage class for the bucket
            bucket.storage_class = "STANDARD"

            # Using "asia-east1" location for whole project
            new_bucket = storage_client.create_bucket(bucket, location="asia-east1")
            print(f"Bucket {new_bucket.name} created successfully.")

            return new_bucket

        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def upload_historical_file_to_gcs(self,url, bucket_name):
        """
        This Function uploads files directly from url into the gcs bucket
        :param bucket_name: The name of bucket into which file will get uploaded
        :return:
        """
        # Fetch data from URL
        """
        The "get" method returns a Response object, which contains:
        response.status_code: The HTTP status code (e.g., 200 for success, 404 for not found).
        response.text: The response body as a string.
        response.json(): Parses the response body as JSON (raises an error if the body isn't valid JSON)."""

        response = requests.get(url)
        try:
            if response.status_code != 200:
                print(f"Failed to download the file from URL: {response.status_code}")
                return

            raw_data = response.json()

            # Define file path with date formatting for GCS storage
            current_date = datetime.now().strftime('%Y%m%d')
            destination_blob_name = f"pyspark/landing/historical/{current_date}/earthquake_historical_data.json"

            # Initialize Google Cloud Storage client
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            # Upload data directly to GCS

            #"response.content" in the requests library refers to the body of the HTTP response as raw bytes(exactly as it was received from the server)
            #Why Use upload_from_string()?
            #Direct Upload: You can upload data directly without writing to a file first in the local system, saving time and resources.

            # Upload the JSON data directly from memory to GCS
            blob.upload_from_string(data=json.dumps(raw_data), content_type='application/json')

            print(f"File uploaded successfully to gs://{bucket_name}/{destination_blob_name}")
            logging.info(f"File uploaded successfully to gs://{bucket_name}/{destination_blob_name}")

        except Exception as e:
            logging.error(f"Error uploading JSON to GCS: {e}")

    def upload_file_to_gcs(self,url, bucket_name):
        """
        This Function uploads files directly from url into the gcs bucket
        :param bucket_name: The name of bucket into which file will get uploaded
        :return:
        """
        # Fetch data from URL
        """
        The "get" method returns a Response object, which contains:
        response.status_code: The HTTP status code (e.g., 200 for success, 404 for not found).
        response.text: The response body as a string.
        response.json(): Parses the response body as JSON (raises an error if the body isn't valid JSON)."""

        response = requests.get(url)
        try:
            if response.status_code != 200:
                print(f"Failed to download the file from URL: {response.status_code}")
                return

            raw_data = response.json()

            # Define file path with date formatting for GCS storage
            current_date = datetime.now().strftime('%Y%m%d')
            destination_blob_name = f"pyspark/landing/{current_date}/earthquake_daily_data.json"

            # Initialize Google Cloud Storage client
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            # Upload data directly to GCS

            #"response.content" in the requests library refers to the body of the HTTP response as raw bytes(exactly as it was received from the server)
            #Why Use upload_from_string()?
            #Direct Upload: You can upload data directly without writing to a file first in the local system, saving time and resources.

            # Upload the JSON data directly from memory to GCS
            blob.upload_from_string(data=json.dumps(raw_data), content_type='application/json')

            print(f"File uploaded successfully to gs://{bucket_name}/{destination_blob_name}")
            logging.info(f"File uploaded successfully to gs://{bucket_name}/{destination_blob_name}")

        except Exception as e:
            logging.error(f"Error uploading JSON to GCS: {e}")


    def download_from_gcs(self,bucket_name, file_path):
        """

        :param file_path:
        :return:
        """
        try:
            # Set up the GCS client
            client = storage.Client()

            # Access the bucket and blob (file)
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(file_path)

            # Download the data as a string and parse it into JSON
            data_string = blob.download_as_text()

            # Convert the JSON string to a Python dictionary
            data_json = json.loads(data_string)

            print(f"Successfully downloaded {file_path} from GCS.")

            return data_json

        except Exception as e:
            print(f"Error downloading {file_path} from GCS: {e}")
            return None

class SparkDataProcessor:
    def __init__(self, app_name):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

        # self.spark = SparkSession.builder \
        #             .appName('app_name') \
        #             .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5") \
        #             .getOrCreate()
    def create_dataframe(self, bucket_name):
        """
        Creates a Spark DataFrame from JSON data in GCS.
        """
        schema = StructType([
            StructField("type", StringType(), True),
            StructField("metadata", StructType([
                StructField("generated", LongType(), True),
                StructField("url", StringType(), True),
                StructField("title", StringType(), True),
                StructField("status", IntegerType(), True),
                StructField("api", StringType(), True),
                StructField("count", IntegerType(), True)
            ]), True),
            StructField("features", ArrayType(StructType([
                StructField("type", StringType(), True),
                StructField("properties", StructType([
                    StructField("mag", FloatType(), True),
                    StructField("place", StringType(), True),
                    StructField("time", StringType(), True),
                    StructField("updated", StringType(), True),
                    StructField("tz", IntegerType(), True),
                    StructField("url", StringType(), True),
                    StructField("detail", StringType(), True),
                    StructField("felt", IntegerType(), True),
                    StructField("cdi", FloatType(), True),
                    StructField("mmi", FloatType(), True),
                    StructField("alert", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("tsunami", IntegerType(), True),
                    StructField("sig", IntegerType(), True),
                    StructField("net", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("ids", StringType(), True),
                    StructField("sources", StringType(), True),
                    StructField("types", StringType(), True),
                    StructField("nst", IntegerType(), True),
                    StructField("dmin", FloatType(), True),
                    StructField("rms", FloatType(), True),
                    StructField("gap", FloatType(), True),
                    StructField("magType", StringType(), True),
                    StructField("title", StringType(), True)
                ]), True),
                StructField("geometry", StructType([
                    StructField("type", StringType(), True),
                    StructField("coordinates", ArrayType(FloatType()), True)
                ]), True),
                StructField("id", StringType(), True)
            ])), True)
        ])
        date_str = datetime.now().strftime('%Y%m%d')
        source_blob_filepath = f'gs://{bucket_name}/pyspark/landing/{date_str}/earthquake_daily_data.json'
        return self.spark.read.json(source_blob_filepath, schema=schema)

    def flatten_df(self, bucket_name):
        """
        Flattens a nested DataFrame by extracting fields and creating separate columns for coordinates.
        """
        df = self.create_dataframe(bucket_name)
        flattened_df = df.select(explode("features").alias("feature")) \
            .select(col("feature.properties.mag").cast("float").alias("mag"),
                    col("feature.properties.place").alias("place"),
                    col("feature.properties.time").alias("time"),
                    col("feature.properties.updated").alias("updated"),
                    col("feature.properties.tz").cast("int").alias("tz"),
                    col("feature.properties.url").alias("url"),
                    col("feature.properties.detail").alias("detail"),
                    col("feature.properties.felt").cast("int").alias("felt"),
                    col("feature.properties.cdi").cast("float").alias("cdi"),
                    col("feature.properties.mmi").cast("float").alias("mmi"),
                    col("feature.properties.alert").alias("alert"),
                    col("feature.properties.status").alias("status"),
                    col("feature.properties.tsunami").cast("int").alias("tsunami"),
                    col("feature.properties.sig").cast("int").alias("sig"),
                    col("feature.properties.net").alias("net"),
                    col("feature.properties.code").alias("code"),
                    col("feature.properties.ids").alias("ids"),
                    col("feature.properties.sources").alias("sources"),
                    col("feature.properties.types").alias("types"),
                    col("feature.properties.nst").cast("int").alias("nst"),
                    col("feature.properties.dmin").cast("float").alias("dmin"),
                    col("feature.properties.rms").cast("float").alias("rms"),
                    col("feature.properties.gap").cast("float").alias("gap"),
                    col("feature.properties.magType").alias("magType"),
                    col("feature.properties.title").alias("title"),
                    col("feature.geometry.type").alias("geometry_type"),
                    col("feature.geometry.coordinates").alias("coordinates")
                    )

        flattened_df = flattened_df.withColumn("longitude", flattened_df["coordinates"].getItem(0)) \
            .withColumn("latitude", flattened_df["coordinates"].getItem(1)) \
            .withColumn("depth", flattened_df["coordinates"].getItem(2)) \
            .drop("coordinates")

        return flattened_df

    def transform_columns(self, bucket_name):
        """
        Transforms the DataFrame by converting epoch time to timestamp, extracting area from place, and adding a current timestamp.
        """
        flattened_df = self.flatten_df(bucket_name)
        return flattened_df.withColumn("time", from_unixtime(col("time") / 1000).cast("timestamp")) \
            .withColumn("updated", from_unixtime(col("updated") / 1000).cast("timestamp")) \
            .withColumn("area", regexp_extract(col("place"), r"of (.+)$", 1)) \
            .withColumn("insert_dt", current_timestamp())


    def save_to_gcs(self, bucket_name, layer="silver"):
        """
        Saves the transformed DataFrame to GCS in Parquet format.
        """
        transformed_df = self.transform_columns(bucket_name)
        date_str = datetime.now().strftime('%Y%m%d')
        gcs_path = f"gs://{bucket_name}/pyspark/{layer}/{date_str}/transformed_data"
        transformed_df.write.parquet(gcs_path, mode='append')
        return gcs_path


    def BigqueryLoad(self, df):
        # Write data to bigquery
        df.write.format("bigquery") \
            .option("table", "swift-terra-440607-n3.earthquake01.earthquake_data-Dataproc") \
            .option("writeMethod", "direct") \
            .mode('append') \
            .save()


class SparkHistoricalDataProcessor:
    def __init__(self, app_name):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def create_dataframe_historical_data(self, bucket_name):
        """
        Creates a Spark DataFrame from JSON data in GCS.
        """
        schema = StructType([
            StructField("type", StringType(), True),
            StructField("metadata", StructType([
                StructField("generated", LongType(), True),
                StructField("url", StringType(), True),
                StructField("title", StringType(), True),
                StructField("status", IntegerType(), True),
                StructField("api", StringType(), True),
                StructField("count", IntegerType(), True)
            ]), True),
            StructField("features", ArrayType(StructType([
                StructField("type", StringType(), True),
                StructField("properties", StructType([
                    StructField("mag", FloatType(), True),
                    StructField("place", StringType(), True),
                    StructField("time", StringType(), True),
                    StructField("updated", StringType(), True),
                    StructField("tz", IntegerType(), True),
                    StructField("url", StringType(), True),
                    StructField("detail", StringType(), True),
                    StructField("felt", IntegerType(), True),
                    StructField("cdi", FloatType(), True),
                    StructField("mmi", FloatType(), True),
                    StructField("alert", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("tsunami", IntegerType(), True),
                    StructField("sig", IntegerType(), True),
                    StructField("net", StringType(), True),
                    StructField("code", StringType(), True),
                    StructField("ids", StringType(), True),
                    StructField("sources", StringType(), True),
                    StructField("types", StringType(), True),
                    StructField("nst", IntegerType(), True),
                    StructField("dmin", FloatType(), True),
                    StructField("rms", FloatType(), True),
                    StructField("gap", FloatType(), True),
                    StructField("magType", StringType(), True),
                    StructField("title", StringType(), True)
                ]), True),
                StructField("geometry", StructType([
                    StructField("type", StringType(), True),
                    StructField("coordinates", ArrayType(FloatType()), True)
                ]), True),
                StructField("id", StringType(), True)
            ])), True)
        ])
        date_str = datetime.now().strftime('%Y%m%d')
        source_blob_filepath = f'gs://{bucket_name}/pyspark/landing/historical/{date_str}/earthquake_historical_data.json'
        return self.spark.read.json(source_blob_filepath, schema=schema)

    def flatten_df(self, bucket_name):
        """
        Flattens a nested DataFrame by extracting fields and creating separate columns for coordinates.
        """
        df = self.create_dataframe_historical_data(bucket_name)
        flattened_df = df.select(explode("features").alias("feature")) \
            .select(col("feature.properties.mag").cast("float").alias("mag"),
                    col("feature.properties.place").alias("place"),
                    col("feature.properties.time").alias("time"),
                    col("feature.properties.updated").alias("updated"),
                    col("feature.properties.tz").cast("int").alias("tz"),
                    col("feature.properties.url").alias("url"),
                    col("feature.properties.detail").alias("detail"),
                    col("feature.properties.felt").cast("int").alias("felt"),
                    col("feature.properties.cdi").cast("float").alias("cdi"),
                    col("feature.properties.mmi").cast("float").alias("mmi"),
                    col("feature.properties.alert").alias("alert"),
                    col("feature.properties.status").alias("status"),
                    col("feature.properties.tsunami").cast("int").alias("tsunami"),
                    col("feature.properties.sig").cast("int").alias("sig"),
                    col("feature.properties.net").alias("net"),
                    col("feature.properties.code").alias("code"),
                    col("feature.properties.ids").alias("ids"),
                    col("feature.properties.sources").alias("sources"),
                    col("feature.properties.types").alias("types"),
                    col("feature.properties.nst").cast("int").alias("nst"),
                    col("feature.properties.dmin").cast("float").alias("dmin"),
                    col("feature.properties.rms").cast("float").alias("rms"),
                    col("feature.properties.gap").cast("float").alias("gap"),
                    col("feature.properties.magType").alias("magType"),
                    col("feature.properties.title").alias("title"),
                    col("feature.geometry.type").alias("geometry_type"),
                    col("feature.geometry.coordinates").alias("coordinates")
                    )

        flattened_df = flattened_df.withColumn("longitude", flattened_df["coordinates"].getItem(0)) \
            .withColumn("latitude", flattened_df["coordinates"].getItem(1)) \
            .withColumn("depth", flattened_df["coordinates"].getItem(2)) \
            .drop("coordinates")

        return flattened_df

    def transform_columns(self, bucket_name):
        """
        Transforms the DataFrame by converting epoch time to timestamp, extracting area from place, and adding a current timestamp.
        """
        flattened_df = self.flatten_df(bucket_name)
        return flattened_df.withColumn("time", from_unixtime(col("time") / 1000).cast("timestamp")) \
            .withColumn("updated", from_unixtime(col("updated") / 1000).cast("timestamp")) \
            .withColumn("area", regexp_extract(col("place"), r"of (.+)$", 1)) \
            .withColumn("insert_dt", current_timestamp())

    def save_to_gcs_historical(self, bucket_name, layer="silver"):
        """
        Saves the transformed DataFrame to GCS in Parquet format.
        """
        transformed_df = self.transform_columns(bucket_name)
        date_str = datetime.now().strftime('%Y%m%d')
        gcs_path = f"gs://{bucket_name}/pyspark/{layer}/{date_str}/transformed_data"
        transformed_df.write.parquet(gcs_path, mode='overwrite')
        return gcs_path

    def BigqueryLoadHistorical(self, df):
        # Write data to bigquery
        df.write.format("bigquery") \
            .option("table", "swift-terra-440607-n3.earthquake01.earthquake_data-Dataproc") \
            .option("writeMethod", "direct") \
            .mode('overwrite') \
            .save()
