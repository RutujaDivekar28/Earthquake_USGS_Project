import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import os

# Set up Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'path/to/your/service_account_key.json'

# Project and bucket information
project_id = 'your-gcp-project-id'
gcs_bucket = 'your-gcs-bucket'
input_table = 'your-gcp-project-id:dataset.table'
output_prefix = f'gs://{gcs_bucket}/output/'

# Define pipeline options
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = project_id
google_cloud_options.region = 'us-central1'
google_cloud_options.staging_location = f'gs://{gcs_bucket}/dataflow/staging/'
google_cloud_options.temp_location = f'gs://{gcs_bucket}/dataflow/temp/'
options.view_as(StandardOptions).runner = 'DataflowRunner'  # Change to 'DirectRunner' for local testing

# Create a Beam pipeline
with beam.Pipeline(options=options) as p:
    # Step 1: Read data from BigQuery
    input_data = (
        p
        | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
            query=f'SELECT user_id, order_id, order_value FROM `{input_table}` WHERE order_status = "COMPLETED"',
            use_standard_sql=True
        )
    )

    # Step 2: Filter orders with value > $100
    filtered_orders = (
        input_data
        | 'FilterHighValueOrders' >> beam.Filter(lambda order: order['order_value'] > 100)
    )

    # Step 3: Calculate the average order value
    average_order_value = (
        filtered_orders
        | 'ExtractOrderValues' >> beam.Map(lambda order: order['order_value'])
        | 'CalculateMean' >> beam.combiners.Mean.Globally()
    )

    # Step 4: Group orders by user ID and sum their total order value
    orders_per_user = (
        filtered_orders
        | 'MapToUserKeyValue' >> beam.Map(lambda order: (order['user_id'], order['order_value']))
        | 'SumValuesPerUser' >> beam.CombinePerKey(sum)
    )

    # Step 5: Split data into large and small orders
    class SplitOrdersFn(beam.DoFn):
        def process(self, element):
            user_id, total_value = element
            if total_value > 500:
                yield beam.pvalue.TaggedOutput('large_orders', (user_id, total_value))
            else:
                yield (user_id, total_value)

    split_orders = (
        orders_per_user
        | 'SplitOrders' >> beam.ParDo(SplitOrdersFn()).with_outputs('large_orders', main='small_orders')
    )

    large_orders = split_orders.large_orders
    small_orders = split_orders.small_orders

    # Step 6: Write outputs to GCS
    (large_orders
     | 'WriteLargeOrdersToGCS' >> beam.io.WriteToText(f'{output_prefix}large_orders', file_name_suffix='.txt')
    )

    (small_orders
     | 'WriteSmallOrdersToGCS' >> beam.io.WriteToText(f'{output_prefix}small_orders', file_name_suffix='.txt')
    )

    # Print the average order value
    average_order_value | 'PrintAverage' >> beam.Map(print)
