import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import os

# Set Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\Rutuja Divekar\PycharmProjects\Earthquake_USGS_Project\swift-terra-440607-n3-95dff25490e1.json"

project_id = "swift-terra-440607-n3"
output_table = "swift-terra-440607-n3:earthquake01.earthquake_data-Dataflow"
GCS_BUCKET = "earthquake-bucket-rsd"

# Initialize pipeline options
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.job_name = "earthquake-analysis"
google_cloud_options.region = "us-central1"
google_cloud_options.staging_location = f"gs://{GCS_BUCKET}/dataflowtemp/stag_loc/"
google_cloud_options.temp_location = f"gs://{GCS_BUCKET}/dataflowtemp/temp_loc/"
options.view_as(StandardOptions).runner = 'DirectRunner'  # Use 'DirectRunner' for local testing


# Create a Beam pipeline
with beam.Pipeline(options=options) as p:
    # Read data from BigQuery
    input_data = (
        p
        | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
            query='SELECT * FROM `swift-terra-440607-n3.earthquake01.earthquake_data-Dataflow`',
            use_standard_sql=True
        )
    )

    # printed_data = (
    #     input_data
    #     | 'PrintData' >> beam.Map(print)
    # )

    # 1. Count the number of earthquakes by region
    earthquakes_by_region = (
        input_data
        | 'MapRegionToKey' >> beam.Map(lambda record: (record['area'], 1))
        | 'CountPerRegion' >> beam.CombinePerKey(sum)
    )

    # earthquakes_by_region | 'PrintRegionCounts' >> beam.Map(print)


    #  2. Find the average magnitude by the region
    magnitude_by_region = (
            input_data
            | 'MapRegionToMagnitude' >> beam.Map(lambda record: (record['area'], record['magnitude']))
            | 'CalculateAverageMagnitude' >> beam.combiners.Mean.PerKey()
    )

    # magnitude_by_region | 'PrintAvgMag' >> beam.Map(print)

    #  3. Find how many earthquakes happen on the same day.
    earthquakes_per_day = (
        input_data
        | 'ExtractDate' >> beam.Map(lambda record: (record['insert_date'][:10], 1))  # Extract only the 'YYYY-MM-DD' part
        | 'CountEarthquakesPerDay' >> beam.CombinePerKey(sum)
    )

    # earthquakes_per_day | 'PrintEarthquakePerDay' >> beam.Map(print)

    #  4. Find how many earthquakes happen on same day and in same region
    #Extract the date (YYYY-MM-DD) and create a composite key of (insert_date, area)
    earthquakes_per_day_region = (
        input_data
        | 'ExtractDateAndRegion' >> beam.Map(lambda record: ((record['insert_date'][:10], record['area']), 1))  # Extract date and use region as key
        | 'CountEarthquakesPerDayRegion' >> beam.CombinePerKey(sum)
    )

    # earthquakes_per_day_region | 'PrintEarthquakePerDayRegion' >> beam.Map(print)

    #  5. Find average earthquakes happen on the same day.
    average_earthquakes_per_day = (
        input_data
        | 'ExtractDate5' >> beam.Map(lambda record: (record['insert_date'][:10], 1))  # Extract date as key
        | 'CountEarthquakesPerDay5' >> beam.CombinePerKey(sum)  # Count earthquakes per day
        | 'CalculateAverage5' >> beam.combiners.Mean.PerKey()  # Calculate the average
    )

    # average_earthquakes_per_day | 'PrintAvgEarthquakePerDay' >> beam.Map(print)


    #  6. Find average earthquakes happen on same day and in same region

    # Group by (insert_date, area) and calculate the mean number of earthquakes per (insert_date, area)
    average_earthquakes_per_day_region = (
        input_data
        | 'CreateDateRegionKey' >> beam.Map(lambda record: ((record['insert_date'][:10], record['area']), 1))  # Group by (date, region)
        | 'CalculateAverage' >> beam.combiners.Mean.PerKey()  # Calculate the average for each (date, region)
    )

    # average_earthquakes_per_day_region | 'PrintAvgEarthquakePerDayRegion' >> beam.Map(print)


    #  7. Find the region name, which had the highest magnitude earthquake last week.

    # Group by region and find the maximum magnitude for each region
    max_magnitude_per_region = (
        input_data
        | 'CreateRegionKey' >> beam.Map(lambda record: (record['area'], record['magnitude']))  # Group by region
        | 'FindMaxMagnitude' >> beam.CombinePerKey(max)  # Find max magnitude for each region
    )

    # Find the region with the highest magnitude earthquake
    highest_magnitude_region = (
        max_magnitude_per_region
        | 'FindHighestMagnitude' >> beam.CombineGlobally(lambda region_mags: max(region_mags, key=lambda x: x[1]))  # Find region with highest magnitude
    )

    # highest_magnitude_region | 'PrintHighest_magnitude_region' >> beam.Map(print)


    #  8. Find the region name, which is having magnitudes higher than 5.

    high_magnitude_earthquakes = (
        input_data
        | 'FilterMagnitudesGreaterThan5' >> beam.Filter(lambda record: record['magnitude'] > 5)
        | 'ExtractRegions8' >> beam.Map(lambda record: record['area'])  # Extract region names
        | 'RemoveDuplicates' >> beam.Distinct()  # Remove duplicate regions
    )

    high_magnitude_earthquakes | 'PrintHigh_magnitude_earthquakes' >> beam.Map(print)


    #  9. Find out the regions which are having the highest frequency and intensity of earthquakes

