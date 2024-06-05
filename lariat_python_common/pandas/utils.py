import duckdb
import logging
import fsspec
from fastavro import reader
import pandas as pd


def run_sql_query(
    df,
    query,
):
    """
    Runs a provided sql query and returns the resulting dataframe response
    """
    logging.info(f"Running query: {query}")
    res = None
    try:
        res = duckdb.query(query).df()
    except Exception as e:
        logging.error(e)
    return res


# Generator function to read Avro file in chunks
def get_df_iterator_from_avro(path, chunksize=None):
    with fsspec.open(path, mode="rb") as f:
        avro_reader = reader(f)
        batch = []
        for record in avro_reader:
            batch.append(record)
            if chunksize and len(batch) >= chunksize:
                yield pd.DataFrame(batch)
                batch = []
        if batch:
            yield pd.DataFrame(batch)
