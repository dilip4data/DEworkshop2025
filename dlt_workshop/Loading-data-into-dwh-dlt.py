# pip install dlt[bigquery]

# Let's use our NY taxi API and load data from the source into destination

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name="rides", write_disposition="replace")
def ny_taxi():
    client = RESTClient(base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net",paginator = PageNumberPaginator(base_page=1, total_path=None))
    for page in client.paginate("data_engineering_zoomcamp_api"):
            yield page

# Test pipeline locally
# pipeline = dlt.pipeline(pipeline_name="taxi_data", destination="duckdb", dataset_name="taxi_rides")

# load_info = pipeline.run(ny_taxi, write_disposition="replace")
# print(load_info)

# explore loaded data
# pipeline.dataset(dataset_type="default").rides.df()

pipeline = dlt.pipeline(
            pipeline_name='taxi_data',
            destination='bigquery',
            dataset_name='taxi_rides',
            dev_mode=True,
)

info = pipeline.run(ny_taxi)
print(info)