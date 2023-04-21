import logging
import io
import random
import time
import pandas as pd
from botocore.exceptions import ClientError
from lariat_python_common.athena.custom_exceptions import (
    AthenaQueryRunException,
    REPORT_EXCEPTION,
)
from genson import SchemaBuilder
import json
from lariat_python_common.athena.schema import generate_create_table
from typing import List
from boto3_type_annotations import athena


MAX_RETRIES = 10
QUERY_FAILURE_THRESHOLD = 1
DF_TYPE_INFERENCE_READ_LIMIT = 100


# S3 constant
S3_QUERY_OUTPUT_BUCKET_NAME = "lariat-athena-query-results"
S3_QUERY_OUTPUT_BUCKET_URI = f"s3://{S3_QUERY_OUTPUT_BUCKET_NAME}`"
DEFAULT_S3_QUERY_OUTPUT_PATH = ""


def add_partition(table_name, params_dict, athena_handler, athena_database):
    params_arr = [f"{key} = '{val}'" for key, val in params_dict.items()]
    query = f"""
    ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (
       {",".join(params_arr)}
    )
    """
    run_athena_query(
        athena_handler,
        athena_database,
        query,
        S3_QUERY_OUTPUT_BUCKET_NAME,
        DEFAULT_S3_QUERY_OUTPUT_PATH,
    )


def get_meta_and_query_from_execution_id(
    query_execution_id, error_state, athena_handler
):
    query_execution = athena_handler.get_query_execution(
        QueryExecutionId=query_execution_id
    )["QueryExecution"]
    meta = {}
    if error_state:
        meta["error"] = {
            "error_message": query_execution["Status"]["StateChangeReason"],
            "submission_time": query_execution["Status"]["SubmissionDateTime"],
            "completion_time": query_execution["Status"]["CompletionDateTime"],
            "athena_error": query_execution["Status"]["AthenaError"],
        }

    meta["statistics"] = query_execution["Statistics"]
    meta["query"] = query_execution["Query"]
    meta["id"] = query_execution_id
    meta["workgroup"] = query_execution["WorkGroup"]
    meta["version"] = query_execution["EngineVersion"]["EffectiveEngineVersion"]
    query = query_execution["Query"]
    return query, meta


def is_glue_crawler_ready(crawler_name, glue_handler):
    response = glue_handler.get_crawler(Name=crawler_name)
    return response["Crawler"]["State"] == "READY"


def wait_till_glue_crawler_ready(crawler_name, glue_handler):
    while not is_glue_crawler_ready(
        crawler_name=crawler_name, glue_handler=glue_handler
    ):
        for i in range(1, 1 + MAX_RETRIES):
            if is_glue_crawler_ready(
                crawler_name=crawler_name, glue_handler=glue_handler
            ):
                return
            time.sleep((2**i) + (random.randint(0, 1000) / 1000))
    return


def does_table_exist(
    athena_handler: athena.Client, table_name: str, db_name: str
) -> bool:
    """
    This is unsafe to use when not in control of table_name and db_name
    :return: true if table exists, false if not
    """
    query = f"""SELECT table_schema, table_name from information_schema.tables where table_name = '{table_name}' and
    table_schema = '{db_name}'"""
    result_key = run_athena_query(
        athena_handler,
        db_name,
        query,
        S3_QUERY_OUTPUT_BUCKET_NAME,
        DEFAULT_S3_QUERY_OUTPUT_PATH,
    )
    df = pd.read_csv(f"s3://{S3_QUERY_OUTPUT_BUCKET_NAME}/{result_key}")
    return not df.empty


def does_crawler_exist(crawler_name, glue_handler):
    try:
        is_glue_crawler_ready(crawler_name=crawler_name, glue_handler=glue_handler)
    except glue_handler.exceptions.EntityNotFoundException:
        return False
    return True


def create_athena_table_from_df(
    df: pd.DataFrame,
    partition_columns: List[str],
    table_name: str,
    db_name: str,
    source_path: str,
    athena_handler: athena.Client,
):
    """
    Create an athena table from a dataframe. Maps the dataframe columns to an athena table and registers
    the source_path (excludes partition keys) along with partition columns for the given dataset.
    :param df: dataframe with representative columns and types for a given dataset
    :param partition_columns: list of partition columns
    :param table_name: Athena Table Name
    :param db_name: Database to store the table under
    :param source_path: fully qualified path NOT including partition columns
    :param athena_handler: athena handler to run queries
    :return:
    """
    builder = SchemaBuilder()
    builder.add_object(
        json.loads(df.head(DF_TYPE_INFERENCE_READ_LIMIT).to_json(orient="records"))
    )
    df_schema = builder.to_schema()["items"]["properties"]
    create_db_query = f"""CREATE DATABASE IF NOT EXISTS `{db_name}`"""
    run_athena_query(
        athena_handler,
        db_name,
        create_db_query,
        S3_QUERY_OUTPUT_BUCKET_NAME,
        DEFAULT_S3_QUERY_OUTPUT_PATH,
    )

    create_table_query = generate_create_table(
        table=table_name,
        database=db_name,
        source_path=source_path,
        jsonschema=df_schema,
        partition_columns=partition_columns,
    )
    run_athena_query(
        athena_handler,
        db_name,
        create_table_query,
        S3_QUERY_OUTPUT_BUCKET_NAME,
        DEFAULT_S3_QUERY_OUTPUT_PATH,
    )


def create_glue_crawler(crawler_name, s3_output_path, glue_handler, database):
    glue_handler.create_crawler(
        Name=crawler_name,
        Role="AWSGlue",
        DatabaseName=database,
        Description="",
        Targets={
            "S3Targets": [
                {"Path": s3_output_path},
            ],
        },
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "DEPRECATE_IN_DATABASE",
        },
        RecrawlPolicy={
            "RecrawlBehavior": "CRAWL_EVERYTHING",
        },
        LineageConfiguration={"CrawlerLineageSettings": "DISABLE"},
        LakeFormationConfiguration={
            "UseLakeFormationCredentials": False,
            "AccountId": "",
        },
    )

    glue_handler.start_crawler(Name=crawler_name)


def run_athena_query(
    athena_handler,
    athena_database,
    query,
    output_bucket,
    output_path,
    workgroup="primary",
):
    """
    Runs a provided athena query and returns the resulting file path
    """

    logging.info(f"Running query: {query}")
    s3_output_location = f"s3://{output_bucket.strip('/')}/{output_path.strip('/')}"
    # Execution
    response = athena_handler.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": athena_database},
        ResultConfiguration={
            "OutputLocation": s3_output_location,
        },
        WorkGroup=workgroup,
    )

    # get query execution id
    query_execution_id = response["QueryExecutionId"]
    logging.info(query_execution_id)
    failure_count = 0
    while failure_count < QUERY_FAILURE_THRESHOLD:
        # get execution status
        for i in range(1, 1 + MAX_RETRIES):
            # get query execution
            query_status = athena_handler.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            query_execution_status = query_status["QueryExecution"]["Status"]["State"]

            if query_execution_status == "SUCCEEDED":
                logging.info("STATUS:" + query_execution_status)
                return f"{output_path.strip('/')}/{query_execution_id}.csv".strip("/")

            if query_execution_status in ["FAILED", "CANCELLED"]:
                logging.warning(query_status)
                failure_count += 1
                if failure_count >= QUERY_FAILURE_THRESHOLD:
                    logging.error(query_status)
                    raise Exception(
                        query_status["QueryExecution"]["Status"]["StateChangeReason"]
                    )
                time.sleep(i)
            else:
                logging.info("STATUS:" + query_execution_status)
                time.sleep(2**i + (random.randint(0, 1000) / 1000))

        athena_handler.stop_query_execution(QueryExecutionId=query_execution_id)
        failure_count += 1
    raise Exception("TIME OVER")


def run_athena_query_async(
    athena_handler, athena_database, query, output_bucket, output_path, workgroup
):
    """
    Runs a provided athena query in async. Exponentially backs off when hitting Athena limits
    """

    logging.info(f"Running query: {query}")
    s3_output_location = f"s3://{output_bucket.strip('/')}/{output_path.strip('/')}"

    should_retry = True
    retry = 0
    query_execution_id = None
    while should_retry:
        should_retry = False
        try:
            response = athena_handler.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": athena_database},
                ResultConfiguration={
                    "OutputLocation": s3_output_location,
                },
                WorkGroup=workgroup,
            )
            query_execution_id = response["QueryExecutionId"]

            query_status = athena_handler.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            query_execution_status = query_status["QueryExecution"]["Status"]["State"]
            if query_execution_status == "FAILED":
                logging.error(f"Query failed immediately: {query_execution_id}")
                raise AthenaQueryRunException(query_execution_id, REPORT_EXCEPTION)
        except ClientError as client_error:
            if client_error.response["Error"]["Code"] == "TooManyRequestsException":
                should_retry = True
                time.sleep(2**retry + (random.randint(0, 1000) / 1000))
                retry += 1
            else:
                raise AthenaQueryRunException(
                    query_execution_id, client_error.response["Error"]["Code"]
                )
    return


def get_athena_results_dataframe(s3_resource, bucket, filepath):
    try:
        logging.debug("creating file object")
        file_obj = s3_resource.Bucket(bucket).Object(key=filepath).get()

        logging.debug("reading dataframe")
        df = pd.read_csv(io.BytesIO(file_obj["Body"].read()), encoding="utf8")

        return df

    except Exception as e:
        logging.error(e)
        raise e
