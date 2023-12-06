import duckdb
import logging


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
