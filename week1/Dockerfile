FROM python:3.12.5

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY Data-Loading-Parquet.py Data-Loading-Parquet.py

ENTRYPOINT [ "python", "Data-Loading-Parquet.py" ]

