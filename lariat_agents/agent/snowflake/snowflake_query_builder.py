import traceback

from lariat_agents.base.base_query_builder import BaseQueryBuilder
import time
import lariat_python_common.sql.utils as lariat_sql_utils
import sqlparse
from boto3_type_annotations import s3
import logging
import lariat_python_common.snowflake.utils as snowflake_utils

from lariat_agents.constants import (
    LARIAT_EVENT_NAME,
    SNOWFLAKE_USER,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SKETCH_TYPE_DISTINCT,
    RESULT_OUTPUT_RESULT_MIN_TS,
    RESULT_OUTPUT_RESULT_MAX_TS,
    RESULT_OUTPUT_LOOKBACK_RANGE_END_TS,
    RESULT_OUTPUT_LOOKBACK_RANGE_START_TS,
)

from typing import List
from lariat_python_common.snowflake import schema as lariat_schema_utils


CATEGORICAL_TYPE = "categorical"
NUMERICAL_TYPE = "numeric"


class SnowflakeQueryBuilder(BaseQueryBuilder):
    def __init__(
        self,
        query_builder_type: str,
        s3_handler: s3.Client,
        lariat_udf_db: str,
        lariat_udf_schema: str,
        sketch_mode: bool = True,
    ):
        self.s3_handler = s3_handler
        self.lariat_udf_db = lariat_udf_db
        self.lariat_udf_schema = lariat_udf_schema
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
        df = snowflake_utils.run_snowflake_query(
            query=schema_query,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            db=db_name,
        )
        for table_name, group in df.groupby("table_name"):
            try:
                information_schema = group.to_dict(orient="records")
                output_json_schema = (
                    lariat_schema_utils.transform_snowflake_to_json_schema(
                        information_schema=information_schema
                    )
                )
                schema_output.append(
                    {
                        "schema": output_json_schema,
                        "raw_dataset_name": f"{db_name}.{table_schema}.{table_name}",
                        "raw_dataset_data_source": self._query_builder_type,
                        "raw_dataset_source_id": source_id,
                        "raw_dataset_event_name": LARIAT_EVENT_NAME,
                    }
                )
            except Exception as e:
                logging.error(f"Failed to transform schema for: {table_name}")
        return schema_output

    def fill_in_expressions_without_sketch_objects(
        self, calculation: str, indicator_id: str, group_fields: str = None
    ):
        """
        This function eschews returning sketches, and instead returns the actual approximate values
        for a given sketch function (either the approx_count_distinct or approx_percentile)
        """
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
                resolved_calculation = "approx_count_distinct({})".format(
                    count_distinct_operand
                )
            elif is_match_decile:
                resolved_calculation = "approx_percentile({},{})".format(
                    decile_operand, decile_value
                )
        resolved_calculation = lariat_sql_utils.safe_cast_calculation(
            sqlparse.parse(resolved_calculation)[0]
        )
        return f"{resolved_calculation} as _indicator_{indicator_id}"

    def fill_in_expressions_with_sketch_objects(
        self, calculation, indicator_id, group_fields: str = None
    ):
        """
        Takes a calculation and indicator id and either creates a predicate of calculation as _indicator_{indicator_id}
        or substitutes COUNT(DISTINCT x) or approx_percentile(x,0.5) with the appropriate sketching operation and
        creates a predicate of type sketch_compute as _indicator_{indicator_id}
        :param calculation: The Calculation for an indicator e.g: COUNT(distinct device_id) or SUM(user_count)
        :param indicator_id: The Lariat ID of the indicator
        :param group_fields: The Group Fields in the indicator - This is needed for allowing multiple values to be returned
        by snowflake UDF
        :return: a string with a resolved expression e.g: "SUM(user_count) as _indicator_5" or
         "to_base64(CAST(APPROX_SET(device_id) as varbinary)) as _indicator_1"
        N.B: This doesn't support sketch percentiles yet.
        """
        statement = sqlparse.parse(calculation)[0]
        resolved_calculation = calculation
        is_group = group_fields and len(group_fields) > 0
        lariat_udf_db = self.lariat_udf_db.removesuffix('"').removeprefix('"')
        lariat_udf_db = f'"{lariat_udf_db}"'
        lariat_udf_schema = self.lariat_udf_schema.removesuffix('"').removeprefix('"')
        lariat_udf_schema = f'"{lariat_udf_schema}"'

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
                if not is_group:
                    resolved_calculation = (
                        f"to_char({lariat_udf_db}.{lariat_udf_schema}.hll_merge("
                        f"array_agg(to_char(sketch, 'base64'))), 'base64') as _indicator_{indicator_id}"
                    )
                    resolved_table_tail = (
                        f"table("
                        f"{lariat_udf_db}.{lariat_udf_schema}.hllpp_count_strings_sketch("
                        f"{count_distinct_operand}::string))"
                    )
                else:
                    resolved_calculation = (
                        f"to_char(sketch,'base64') as _indicator_{indicator_id}"
                    )
                    resolved_table_tail = (
                        f"table({lariat_udf_db}.{lariat_udf_schema}.hllpp_count_strings_sketch("
                        f"{count_distinct_operand}::string) "
                        f"OVER (PARTITION BY  {','.join(group_fields)}))"
                    )
                return resolved_calculation, resolved_table_tail, SKETCH_TYPE_DISTINCT
            """
            elif is_match_decile:
                resolved_calculation = (
                    "to_base64(CAST(qdigest_agg({}) as varbinary))".format(decile_operand)
                )
            """
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
    ) -> str:
        """
        In the sketch case, sketch functions are delegated to udfs and group variables need to be added to the end
        of the query
        """
        select_predicate = list(map(self.construct_select_predicate, group_fields))
        table_tail = None
        expr_type = None
        for calculation, indicator_id in calculation_indicator_id_pairs:
            if self.sketch_mode:
                filled_in_expression = self.fill_in_expressions_with_sketch_objects(
                    calculation, indicator_id, group_fields
                )
            else:
                filled_in_expression = self.fill_in_expressions_without_sketch_objects(
                    calculation, indicator_id, group_fields
                )
            table_tail = None
            expr_type = None
            if type(filled_in_expression) == tuple and len(filled_in_expression) == 3:
                table_tail = filled_in_expression[1]
                expr_type = filled_in_expression[2]
                filled_in_expression = filled_in_expression[0]
            select_predicate.append(filled_in_expression)

        where_predicate = ""
        if timestamp_field:
            if evaluation_time is None:
                evaluation_time = round(time.time())
            if lookback_time is None:
                lookback_time = 0
            select_predicate.append(
                self.snowflake_timestamp_fields(
                    timestamp_field, evaluation_time, lookback_time, expr_type
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
            if not expr_type:
                group_predicate = f"GROUP BY {','.join(group_fields)}"
            elif expr_type == SKETCH_TYPE_DISTINCT:
                group_predicate = ""

        if not expr_type:
            query = (
                f"SELECT {','.join(select_predicate)} FROM ({computed_dataset_query})"
            )
        elif expr_type == SKETCH_TYPE_DISTINCT:
            query = f"SELECT {','.join(select_predicate)} FROM ({computed_dataset_query} {where_predicate})"
        else:
            query = (
                f"SELECT {','.join(select_predicate)} FROM ({computed_dataset_query})"
            )
        if table_tail:
            query = f"{query}, {table_tail}"
        if not expr_type:
            query = f"{query} {where_predicate} {group_predicate}"
        else:
            query = f"{query} {group_predicate}"

        # Remove leading and trailing whitespaces for the sake of cleaner testing
        return query.strip()

    def run(self, query, output_path):
        logging.debug(f"Running Query: {query}")
        logging.debug(f"Writing Query to: {output_path}")
        output_df = None
        try:
            output_df = snowflake_utils.run_snowflake_query(
                query=query,
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
            )
            indicator_statuses = self.construct_indicator_statuses_from_meta(
                query=query, meta_dict={}
            )
        except Exception:
            meta = {"error": {"error_message": traceback.format_exc()}}
            indicator_statuses = self.construct_indicator_statuses_from_meta(
                query=query, meta_dict=meta
            )
        return output_df, indicator_statuses

    @staticmethod
    def snowflake_timestamp_fields(
        timestamp_col: str,
        evaluation_time: int,
        lookback_time: int,
        sketch_type: str,
    ):
        """
        Constructs part of the query to return the relevant timestamp metadata fields
        and the range of timestamp_col actually available in the inspected data for each sub group
        :param timestamp_col: timestamp column of interest in the query
        :param evaluation_time: time for which the evaluation occurs
        :param lookback_time: how much data to look back on
        :param sketch_type: the type of sketch function this is - timestamps aren't available in the distinct case
        :return: a string of the relevant timestamp queries e.g: MIN(received_time) as _result_min_ts,
                    MAX(received_time) as _result_max_ts, 1654646400 as _lookback_range_end_ts,
                    1654642800 as _lookback_range_start_ts
        """
        if not sketch_type or sketch_type != SKETCH_TYPE_DISTINCT:
            return (
                f"MIN({timestamp_col}) as {RESULT_OUTPUT_RESULT_MIN_TS}, MAX({timestamp_col}) as"
                f" {RESULT_OUTPUT_RESULT_MAX_TS}, {evaluation_time} as {RESULT_OUTPUT_LOOKBACK_RANGE_END_TS},"
                f" {evaluation_time - lookback_time} as {RESULT_OUTPUT_LOOKBACK_RANGE_START_TS}"
            )
        else:
            return (
                f"{evaluation_time - lookback_time} as {RESULT_OUTPUT_RESULT_MIN_TS}, {evaluation_time} as"
                f" {RESULT_OUTPUT_RESULT_MAX_TS}, {evaluation_time} as {RESULT_OUTPUT_LOOKBACK_RANGE_END_TS},"
                f" {evaluation_time - lookback_time} as {RESULT_OUTPUT_LOOKBACK_RANGE_START_TS}"
            )
