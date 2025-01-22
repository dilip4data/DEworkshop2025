#Cleaned up version of data-loading-Parquet.ipynb
import pandas as pd
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine
import argparse, os, sys

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    tb1 = params.tb[0]
    tb2 = params.tb[1]
    url1 = params.url[0]
    url2 = params.url[1]
    
    # Get the name of the file from url
    file_name_pq = url1.rsplit('/',1)[-1].strip()
    print(f'Downloading parquet datafile {file_name_pq} ...')

    # Download file from url1
    os.system(f'curl {url1.strip()} -o {file_name_pq}')
    print('\n')

    file_name_csv = url2.rsplit('/',1)[-1].strip()
    print(f'Downloading taxi zone CSV file {file_name_csv} ...')

    # Download file from url2
    os.system(f'curl {url2.strip()} -o {file_name_csv}')
    print('\n')

    # Create SQL engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read file based on csv or parquet
    if '.csv' in file_name_csv:
        print('CSV yay!')
        print('\n')
        df = pd.read_csv(file_name_csv, nrows=10)
        df_iter = pd.read_csv(file_name_csv, iterator=True, chunksize=100000)

        # Create CSV taxizone table
        df.to_sql(name=tb2, con=engine, if_exists='replace')

    if '.parquet' in file_name_pq:
        print('Parquet oh yeahh!')
        print('\n')
        file = pq.ParquetFile(file_name_pq)
        df = next(file.iter_batches(batch_size=10)).to_pandas()
        df_iter = file.iter_batches(batch_size=100000)

        # Create the green taxi table
        df.head(0).to_sql(name=tb1, con=engine, if_exists='replace')

    # Insert values into the table 
    t_start = time()
    count = 0

    for batch in df_iter:
        count += 1

        if '.parquet' in file_name_pq:
            batch_df = batch.to_pandas()
        else:
            batch_df = batch

        print(f'Inserting batch {count}....')

        b_start = time()
        batch_df.to_sql(name=tb1, con=engine, if_exists='append')
        b_end = time()

        print(f'Inserted! time taken {b_end - b_start:10.3f} seconds. \n')

    t_end = time()

    print(f'Completed! Total time taken was {t_end - t_start:10.3f} seconds for {count} batches.')
    print('\n')





if __name__ == '__main__':
    #Parsing arguments

    parser = argparse.ArgumentParser(description='Loading data from .parquet file to a postgres database')

    parser.add_argument('--user', help='username for Postgres.')
    parser.add_argument('--password', help='password for Postgres username.')
    parser.add_argument('--host', help='hostname for Postgres.')
    parser.add_argument('--port', help='port for Postgres connection.')
    parser.add_argument('--db', help='database name for Postgres.')
    parser.add_argument('--tb', nargs=2, help='target table name sin Postgres.')
    parser.add_argument('--url', nargs=2, help='Source Input files.')

    args = parser.parse_args()

    main(args)