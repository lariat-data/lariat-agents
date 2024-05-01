from lariat_agents.base.batch_base.batch_base_query_builder import BatchBaseQueryBuilder
import time
import lariat_python_common.sql.utils as lariat_sql_utils
import sqlparse
from boto3_type_annotations import athena, s3
import logging
import lariat_python_common.athena.utils as athena_utils
from lariat_python_common.athena.custom_exceptions import (
    AthenaQueryRunException,
    REPORT_EXCEPTION,
)
from lariat_agents.constants import LARIAT_EVENT_NAME

from typing import List, Dict
from lariat_python_common.athena import schema as lariat_schema_utils
import pandas as pd
import io


class AthenaQueryBuilder(BatchBaseQueryBuilder):
    def __init__(
        self,
        query_builder_type: str,
        workgroup_name: str,
        athena_query_bucket_name: str,
        database_name: str,
        athena_handler: athena.Client,
        s3_handler: s3.Client,
        sketch_mode: bool = True,
    ):
        self.workgroup_name = workgroup_name
        self.athena_query_bucket_name = athena_query_bucket_name
        self.default_database_name = database_name
        self.athena_handler = athena_handler
        self.s3_handler = s3_handler
        super().__init__(query_builder_type=query_builder_type, sketch_mode=sketch_mode)

    def run_schema_retrieval(
        self,
        table_schema: str,
        table_names: List[str],
        source_id: str,
        db_name: str = None,
    ):
        schema_output = []
        schema_query = lariat_schema_utils.get_schema_retrieval_query(
            table_names, table_schema
        )
        logging.debug(f"Schema Query: {schema_query}")
        query_execution_key = athena_utils.run_athena_query(
            athena_handler=self.athena_handler,
            athena_database=self.default_database_name,
            query=schema_query,
            output_bucket=self.athena_query_bucket_name,
            output_path="",
        )
        s3_client = self.s3_handler
        s3_response = s3_client.get_object(
            Bucket=self.athena_query_bucket_name, Key=query_execution_key
        )

        overall_df = pd.read_csv(
            io.BytesIO(s3_response["Body"].read()), encoding="utf8"
        )
        df = overall_df.drop("extra_info", axis=1)
        for table_name, group in df.groupby("table_name"):
            try:
                information_schema = group.to_dict(orient="records")
                output_json_schema = (
                    lariat_schema_utils.transform_athena_to_json_schema(
                        information_schema=information_schema
                    )
                )
                partition_keys = (
                    overall_df[
                        (overall_df.extra_info == "partition key")
                        & (overall_df.table_name == table_name)
                    ]["column_name"]
                    .unique()
                    .tolist()
                )
                schema_output.append(
                    {
                        "schema": output_json_schema,
                        "raw_dataset_name": f"{table_schema}.{table_name}",
                        "raw_dataset_data_source": self._query_builder_type,
                        "raw_dataset_source_id": source_id,
                        "raw_dataset_event_name": LARIAT_EVENT_NAME,
                        "meta": {"partition_keys": partition_keys},
                    }
                )
            except Exception as e:
                logging.error(f"Failed to transform schema for: {table_name}")
        return schema_output

    def fill_in_expressions_without_sketch_objects(
        self, calculation: str, indicator_id: str, group_fields: str = None
    ):
        statement = sqlparse.parse(calculation)[0]
        resolved_calculation = calculation
        if not len(statement.tokens) > 1:
            (
                is_count_distinct,
                count_distinct_operand,
            ) = lariat_sql_utils.match_count_distinct_get_operand(statement=statement)
            (
                is_match_decile,
                decile_operand,
                decile_value,
            ) = lariat_sql_utils.match_decile_get_operand(statement=statement)
            if is_count_distinct:
                resolved_calculation = "approx_distinct({})".format(
                    count_distinct_operand
                )
            elif is_match_decile:
                resolved_calculation = "approx_percentile({},{}) as varbinary))".format(
                    decile_operand, decile_value
                )
        resolved_calculation = lariat_sql_utils.safe_cast_calculation(
            sqlparse.parse(resolved_calculation)[0]
        )
        return f"{resolved_calculation} as _indicator_{indicator_id}"

    def fill_in_expressions_with_sketch_objects(
        self, calculation: str, indicator_id: str, group_fields: str = None
    ):
        statement = sqlparse.parse(calculation)[0]
        resolved_calculation = calculation
        if not len(statement.tokens) > 1:
            (
                is_count_distinct,
                count_distinct_operand,
            ) = lariat_sql_utils.match_count_distinct_get_operand(statement=statement)
            (
                is_match_decile,
                decile_operand,
                decile_value,
            ) = lariat_sql_utils.match_decile_get_operand(statement=statement)
            if is_count_distinct:
                resolved_calculation = (
                    "to_base64(CAST(APPROX_SET({}) as varbinary))".format(
                        count_distinct_operand
                    )
                )
            elif is_match_decile:
                resolved_calculation = (
                    "to_base64(CAST(qdigest_agg({}) as varbinary))".format(
                        decile_operand
                    )
                )
        resolved_calculation = lariat_sql_utils.safe_cast_calculation(
            sqlparse.parse(resolved_calculation)[0]
        )
        return f"{resolved_calculation} as _indicator_{indicator_id}"

    def build(
        self,
        computed_dataset_query: str,
        calculation_indicator_id_pairs: str,
        group_fields: str,
        timestamp_field: str,
        evaluation_time: int,
        lookback_time: int,
        filter_str: str,
        name_data_map: Dict = None,
        raw_dataset_names: List = None,
    ) -> str:
        select_predicate = list(map(self.construct_select_predicate, group_fields))
        for calculation, indicator_id in calculation_indicator_id_pairs:
            if self.sketch_mode:
                filled_in_expression = self.fill_in_expressions_with_sketch_objects(
                    calculation, indicator_id
                )
            else:
                filled_in_expression = self.fill_in_expressions_without_sketch_objects(
                    calculation, indicator_id
                )
            select_predicate.append(filled_in_expression)

        where_predicate = ""
        if timestamp_field:
            if evaluation_time is None:
                evaluation_time = round(time.time())
            if lookback_time is None:
                lookback_time = 0
            select_predicate.append(
                self.add_timestamp_fields(
                    timestamp_field, evaluation_time, lookback_time
                )
            )

            where_predicate = (
                f"WHERE {timestamp_field} >= {evaluation_time - lookback_time} AND"
                f" {timestamp_field} < {evaluation_time}"
            )
            if filter_str:
                where_predicate = f"{where_predicate} AND {filter_str}"
        elif filter_str:
            where_predicate = f"WHERE {filter_str}"
        group_predicate = ""
        if len(group_fields) > 0:
            group_predicate = f"GROUP BY {','.join(group_fields)}"

        query = f"SELECT {','.join(select_predicate)} FROM ({computed_dataset_query})"
        query = f"{query} {where_predicate} {group_predicate}"

        # Remove leading and trailing whitespaces for the sake of cleaner testing
        return query.strip()

    def run(self, query, output_path):
        logging.debug(f"Running Query: {query}")
        logging.debug(f"Writing Query to: {output_path}")
        indicator_statuses = []
        try:
            athena_utils.run_athena_query_async(
                athena_handler=self.athena_handler,
                athena_database=self.default_database_name,
                query=query,
                output_bucket=self.athena_query_bucket_name,
                output_path=output_path,
                workgroup=self.workgroup_name,
            )
        except AthenaQueryRunException as query_exception:
            if (query_exception.query_execution_id is not None) and (
                query_exception.exception_type == REPORT_EXCEPTION
            ):
                query, meta = athena_utils.get_meta_and_query_from_execution_id(
                    query_execution_id=query_exception.query_execution_id,
                    error_state=True,
                    athena_handler=self.athena_handler,
                )
                indicator_statuses = self.construct_indicator_statuses_from_meta(
                    query=query, meta_dict=meta
                )
        return None, indicator_statuses
