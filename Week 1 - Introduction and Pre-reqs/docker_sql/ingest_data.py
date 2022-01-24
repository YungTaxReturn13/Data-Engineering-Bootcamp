import pandas as pd
from sqlalchemy import create_engine
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db_name
    table = params.table_name
    url = params.url
    csv_name = "output.csv"

    # download the csv
    os.system(f"curl {url} -o {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    pd.io.sql.get_schema(df, name=table, con=engine)

    df.head(0).to_sql(name=table, con=engine, if_exists="replace")

    df.to_sql(name=table, con=engine, if_exists="append")

    while True:

        df = next(df_iter)

        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])

        df.to_sql(name=table, con=engine, if_exists="append")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    # User
    # Password
    # Host
    # Port
    # Database Name
    # Table Name
    # URL of the CSV

    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db_name", help="data base name for postgres")
    parser.add_argument(
        "--table_name", help="table name where results will be written to"
    )
    parser.add_argument("--url", help="url of the csv file")

    args = parser.parse_args()

    main(args)
