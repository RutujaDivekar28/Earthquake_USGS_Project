from datetime import datetime

################################################################################################
# Name : Config_pyspark.py
# Description : Contains all the variables which can be configured by developer
# Date : 2024 - 11 - 10
# Version : 3.3.2
# Modified date : 2024 - 11 - 10
# Modification Description : N/A
################################################################################################


GOOGLE_APPLICATION_CREDENTIALS = r"C:\Users\Rutuja Divekar\PycharmProjects\Earthquake_USGS_Project\swift-terra-440607-n3-95dff25490e1.json"

Project_id = "swift-terra-440607-n3"

Bucket_name = "earthquake-bucket-rsd"

historical_data_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

daily_data_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


### FILE PATHS TO READ AND WRITE

# Create a unique file name with the current date
current_date = datetime.now().strftime('%Y%m%d')


READ_JSON_FROM_CLOUD = f"pyspark/landing/{current_date}/earthquake_daily_data.json"

READ_HISTORICAL_JSON_FROM_CLOUD = f"pyspark/landing/historical/{current_date}/earthquake_historical_data.json"
# earthquake-bucket-rsd/pyspark/landing/20241110/earthquake_daily_data.json
# READ_JSON_FROM_CLOUD_DAILY = f"landing_layer/daily{date_str}.json"

## Config Variables for Analysis

Dataset = 'earthquake01'

Table = 'earthquake_data-Dataproc'