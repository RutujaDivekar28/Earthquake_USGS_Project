

################################################################################################
# Name : historical_data_loading_using_pyspark.py
# Description : Contains all common codes throughout the Earthquake ingestion project
# Date : 2024 - 10 - 23
# Version : 3.3.2
# Modified date : 2024 - 10 - 23
# Modification Description : we are going to use this module for loading historical data from the url
################################################################################################

import util
import Config_pyspark

if __name__ == '__main__':
    # Initialization of Google application
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = Config.GOOGLE_APPLICATION_CREDENTIALS

    Utils=util.Utils()
    Dataframe = util.SparkHistoricalDataProcessor(app_name='EarthquakeHistoricalDataProcessor')

    # Upload data from URL to Google Cloud Storage in the specified format
    Utils.upload_historical_file_to_gcs(url=Config_pyspark.historical_data_url, bucket_name=Config_pyspark.Bucket_name)

    # Read Data from GCS in JSON dict format
    data_json = Utils.download_from_gcs(bucket_name=Config_pyspark.Bucket_name, file_path=Config_pyspark.READ_HISTORICAL_JSON_FROM_CLOUD)

    df = Dataframe.create_dataframe_historical_data(bucket_name=Config_pyspark.Bucket_name)

    df.show()

    flatten_df = Dataframe.flatten_df(bucket_name=Config_pyspark.Bucket_name)

    transformed_df = Dataframe.transform_columns(bucket_name=Config_pyspark.Bucket_name)

    save_parquet = Dataframe.save_to_gcs_historical(bucket_name=Config_pyspark.Bucket_name)

    load_to_bigquery = Dataframe.BigqueryLoadHistorical(df=transformed_df)








