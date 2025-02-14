import dlt

@dlt.resource(name="sample", write_disposition="replace")
def generate_rows(nr):
    for i in range(nr):
        yield {'id': i, 'example_string': 'abc'}

pipeline = dlt.pipeline(

        pipeline_name="rows_pipeline",
        destination = "duckdb",
        dataset_name="rows_data",
)

# load a single resource
pipeline.run(generate_rows(10))

# load a list of resources
pipeline.run([generate_rows(10), generate_rows(20)])