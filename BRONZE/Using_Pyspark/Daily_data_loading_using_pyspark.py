

################################################################################################
# Name : Daily_data_loading_using_pyspark.py
# Description : Contains all common codes throughout the Earthquake ingestion project
# Date : 2024 - 10 - 24
# Version : 3.3.2
# Modified date : 2024 - 10 - 24
# Modification Description : we are going to use this module for loading daily data from the url
################################################################################################


import os
import util
import Config_pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # Initialization of Google application
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Config.GOOGLE_APPLICATION_CREDENTIALS

    Utils=util.Utils()
    Dataframe = util.SparkDataProcessor(app_name='EarthquakeDataProcessor')

    # Upload data from URL to Google Cloud Storage in the specified format
    Utils.upload_file_to_gcs(url=Config_pyspark.daily_data_url, bucket_name=Config_pyspark.Bucket_name)

    # Read Data from GCS in JSON dict format
    data_json = Utils.download_from_gcs(bucket_name=Config_pyspark.Bucket_name, file_path=Config_pyspark.READ_JSON_FROM_CLOUD)

    df = Dataframe.create_dataframe(bucket_name=Config_pyspark.Bucket_name)

    df.show()

    flatten_df = Dataframe.flatten_df(bucket_name=Config_pyspark.Bucket_name)

    transformed_df = Dataframe.transform_columns(bucket_name=Config_pyspark.Bucket_name)

    save_parquet = Dataframe.save_to_gcs(bucket_name=Config_pyspark.Bucket_name)

    load_to_bigquery = Dataframe.BigqueryLoad(df=transformed_df)








