#################################################################################
# Name : Load Historical Data By DATAFLOW.py
# Description : Load the data after flattening
# Date : 2024 - 11 - 14
# Modified date :
# Modification Description : Added Parquet schema and BigQuery schema.
#################################################################################

# Import required libraries
import apache_beam as beam
import requests
import os
import json
from datetime import datetime
from apache_beam.io import WriteToText, WriteToBigQuery, BigQueryDisposition
from apache_beam.io.parquetio import WriteToParquet
import pyarrow as pa
import logging
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set Google Cloud credentials
os.environ[
    'GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\Rutuja Divekar\PycharmProjects\Earthquake_USGS_Project\swift-terra-440607-n3-95dff25490e1.json"

project_id = "swift-terra-440607-n3"
output_table = "swift-terra-440607-n3:earthquake01.earthquake_data-Dataflow"
GCS_BUCKET = "earthquake-bucket-rsd"
date_str = datetime.now().strftime('%Y%m%d')

# Initialize pipeline options
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.job_name = "earthquakeproject"
google_cloud_options.region = "us-central1"
google_cloud_options.staging_location = f"gs://{GCS_BUCKET}/dataflowtemp/stag_loc/"
google_cloud_options.temp_location = f"gs://{GCS_BUCKET}/dataflowtemp/temp_loc/"
options.view_as(StandardOptions).runner = 'DirectRunner'  # Use 'DirectRunner' for local testing

# Define Parquet schema
parquet_schema = pa.schema([
    ('magnitude', pa.float64()),
    ('place', pa.string()),
    ('area', pa.string()),
    ('time', pa.string()),
    ('updated', pa.string()),
    ('insert_date', pa.string()),
    ('tz', pa.int64()),
    ('url', pa.string()),
    ('detail', pa.string()),
    ('felt', pa.int64()),
    ('cdi', pa.float64()),
    ('mmi', pa.float64()),
    ('alert', pa.string()),
    ('status', pa.string()),
    ('tsunami', pa.int64()),
    ('sig', pa.int64()),
    ('net', pa.string()),
    ('code', pa.string()),
    ('ids', pa.string()),
    ('sources', pa.string()),
    ('types', pa.string()),
    ('nst', pa.int64()),
    ('dmin', pa.float64()),
    ('rms', pa.float64()),
    ('gap', pa.float64()),
    ('magType', pa.string()),
    ('type', pa.string()),
    ('title', pa.string()),
    ('latitude', pa.float64()),
    ('longitude', pa.float64()),
    ('depth', pa.float64())
])

# Define BigQuery schema
bq_schema = {
    "fields": [
        {"name": "magnitude", "type": "FLOAT"},
        {"name": "place", "type": "STRING"},
        {"name": "area", "type": "STRING"},
        {"name": "time", "type": "STRING"},
        {"name": "updated", "type": "STRING"},
        {"name": "insert_date", "type": "STRING"},
        {"name": "tz", "type": "INTEGER"},
        {"name": "url", "type": "STRING"},
        {"name": "detail", "type": "STRING"},
        {"name": "felt", "type": "INTEGER"},
        {"name": "cdi", "type": "FLOAT"},
        {"name": "mmi", "type": "FLOAT"},
        {"name": "alert", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "tsunami", "type": "INTEGER"},
        {"name": "sig", "type": "INTEGER"},
        {"name": "net", "type": "STRING"},
        {"name": "code", "type": "STRING"},
        {"name": "ids", "type": "STRING"},
        {"name": "sources", "type": "STRING"},
        {"name": "types", "type": "STRING"},
        {"name": "nst", "type": "INTEGER"},
        {"name": "dmin", "type": "FLOAT"},
        {"name": "rms", "type": "FLOAT"},
        {"name": "gap", "type": "FLOAT"},
        {"name": "magType", "type": "STRING"},
        {"name": "type", "type": "STRING"},
        {"name": "title", "type": "STRING"},
        {"name": "latitude", "type": "FLOAT"},
        {"name": "longitude", "type": "FLOAT"},
        {"name": "depth", "type": "FLOAT"}
    ]
}


# Function to get data from API in JSON
def get_earthquake_data():
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch data from API: {e}")
        return None


data = get_earthquake_data()


# Function to extract necessary data
def get_necessary_data(feature):
    from datetime import datetime
    if isinstance(feature, str):
        feature = json.loads(feature)
    insert_date = datetime.utcnow().isoformat()

    properties = feature.get("properties", {})
    geometry = feature.get("geometry", {})
    coordinates = geometry.get("coordinates", [None, None, None])

    time_chng = properties.get("time")
    updated_chng = properties.get("updated")
    time = (
        datetime.fromtimestamp(time_chng / 1000).strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(time_chng, int) else None
    )
    updated = (
        datetime.fromtimestamp(updated_chng / 1000).strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(updated_chng, int) else None
    )

    place = properties.get("place")
    area = ', '.join(place.split(",")[1:]).strip() if place else None

    return {
        "magnitude": properties.get("mag"),
        "place": properties.get("place"),
        "area": area,
        "time": time,
        "updated": updated,
        "insert_date": insert_date,
        "tz": properties.get("tz"),
        "url": properties.get("url"),
        "detail": properties.get("detail"),
        "felt": properties.get("felt"),
        "cdi": properties.get("cdi"),
        "mmi": properties.get("mmi"),
        "alert": properties.get("alert"),
        "status": properties.get("status"),
        "tsunami": properties.get("tsunami"),
        "sig": properties.get("sig"),
        "net": properties.get("net"),
        "code": properties.get("code"),
        "ids": properties.get("ids"),
        "sources": properties.get("sources"),
        "types": properties.get("types"),
        "nst": properties.get("nst"),
        "dmin": properties.get("dmin"),
        "rms": properties.get("rms"),
        "gap": properties.get("gap"),
        "magType": properties.get("magType"),
        "type": properties.get("type"),
        "title": properties.get("title"),
        "latitude": coordinates[1],
        "longitude": coordinates[0],
        "depth": coordinates[2] if len(coordinates) > 2 else None
    }

    # Add pipelines with appropriate error handling
if data:
    logger.info("Starting pipeline for loading data into GCS...")
    try:
        with beam.Pipeline(options=options) as p:
            (
                    p
                    | "Fetch Earthquake Data" >> beam.Create([get_earthquake_data()])
                    | "Convert to JSON String" >> beam.Map(json.dumps)
                    | "Write to GCS" >> beam.io.WriteToText(
                f"gs://{GCS_BUCKET}/dataflow/landing/daily/{date_str}/earthquake_daily_data",
                file_name_suffix=".json"
            )
            )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

    logger.info("Starting pipeline for transforming and writing Parquet data...")
    try:
        features = data.get("features", [])
        with beam.Pipeline(options=options) as p:
            (
                    p
                    | "Create Data" >> beam.Create(features)
                    | "Extract Data" >> beam.Map(get_necessary_data)
                    | "Write To Parquet" >> WriteToParquet(
                f"gs://{GCS_BUCKET}/dataflow/silver/daily/{date_str}/flatten_earthquake_data",
                schema=parquet_schema,
                file_name_suffix=".parquet",
                codec="snappy"
            )
            )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

    logger.info("Starting pipeline for loading data into BigQuery...")
    try:
        with beam.Pipeline(options=options) as p:
            (
                    p
                    | "Read From Silver Layer" >> beam.io.ReadFromParquet(
                f"gs://{GCS_BUCKET}/dataflow/silver/daily/{date_str}/flatten_earthquake_data-*"
            )
                    | "Write To BigQuery" >> beam.io.WriteToBigQuery(
                output_table,
                schema=bq_schema,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=f"gs://{GCS_BUCKET}/dataflowtemp/temp_loc/"
            )
            )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")