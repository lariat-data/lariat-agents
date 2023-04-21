import snowflake.connector


def run_snowflake_query(
    query: str,
    user: str,
    password: str,
    account: str,
    db: str = None,
    warehouse: str = None,
):
    if warehouse is None:
        con = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
        )
    else:
        con = snowflake.connector.connect(
            warehouse=warehouse,
            user=user,
            password=password,
            account=account,
        )

    cur = con.cursor()
    if db is not None:
        use_db_query = f"USE DATABASE {db};"
        cur.execute(use_db_query)
    cur.execute(query)
    output_df = cur.fetch_pandas_all()
    output_df.columns = [col.lower() for col in output_df.columns]
    return output_df
