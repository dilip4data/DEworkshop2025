import dlt

data = [
    {
        "vendor_name": "VTS",
        "record_hash": "b00361a396177a9cb410ff61f20015ad",
        "time": {
            "pickup": "2009-06-14 23:23:00",
            "dropoff": "2009-06-14 23:48:00"
        },
        "coordinates": {
            "start": {"lon": -73.787442, "lat": 40.641525},
            "end": {"lon": -73.980072, "lat": 40.742963}
        },
        "passengers": [
            {"name": "John", "rating": 4.9},
            {"name": "Jack", "rating": 3.9}
        ]
    }
]
# Define a dlt pipeline with automatic normalization

pipeline = dlt.pipeline(
        pipeline_name="ny_taxi_data",
        destination="duckdb",
        dataset_name="taxi_rides"
)

# Run the pipeline with raw nested data
info = pipeline.run(data, table_name="rides", write_disposition="replace")

# Print the load summary
print(info)

print(pipeline.last_trace)

