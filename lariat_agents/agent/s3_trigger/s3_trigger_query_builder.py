import time
from typing import List, Tuple, Dict

import pandas as pd

from lariat_agents.base.base_query_builder import BaseQueryBuilder
import logging
import lariat_python_common.sql.utils as lariat_sql_utils
import sqlparse
import lariat_python_common.pandas.utils as pandas_sql_utils
from sqlglot import parse_one
import lariat_python_common.string.utils as lariat_string_utils
import re


CATEGORICAL_TYPE = "categorical"
NUMERICAL_TYPE = "numeric"


class S3TriggerQueryBuilder(BaseQueryBuilder):
    def __init__(
        self,
        query_builder_type: str,
        sketch_mode: bool = True,
        name_data_map: Dict = None,
        raw_dataset_names: List = None,
        data_df: pd.DataFrame = None,
    ):
        self.name_data_map = name_data_map
        self.raw_dataset_names = raw_dataset_names
        self.data_df = data_df
        super().__init__(query_builder_type=query_builder_type, sketch_mode=sketch_mode)

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
                resolved_calculation = "DISTINCT {}".format(count_distinct_operand)
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
                resolved_calculation = "TO_BASE64(CAST(CAST(COUNT(DISTINCT {}) AS varchar) AS bytea))".format(
                    count_distinct_operand
                )
            elif is_match_decile:
                resolved_calculation = "TO_BASE64(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {}) AS bytea))".format(
                    decile_operand
                )
        resolved_calculation = lariat_sql_utils.safe_cast_calculation(
            sqlparse.parse(resolved_calculation)[0]
        )
        return f"{resolved_calculation} as _indicator_{indicator_id}"

    def run_schema_retrieval(
        self,
        table_schema: str,
        table_names: List[str],
        source_id: str,
        db_name: str = None,
    ):
        pass

    @staticmethod
    def construct_indicator_statuses_from_meta(query, meta_dict):
        indicator_statuses = []
        (
            indicator_list,
            evaluation_time,
        ) = S3TriggerQueryBuilder.get_indicators_from_query_str(query)
        for indicator in indicator_list:
            indicator_record = {
                "indicator_id": indicator,
                "evaluation_time": evaluation_time,
                "meta": meta_dict,
            }

            indicator_statuses.append(indicator_record)
        return indicator_statuses

    @staticmethod
    def get_indicators_from_query_str(query: str):
        """
        :param query: Query run by agent
        :return: indicator_list: Integer ids for all indicators part of this query.
                 evaluation_time: Integer Unix Time for Lariat evaluation time for this query
        """
        indicator_list = re.findall("AS _indicator_(\\d+)", query)
        evaluation_time = re.findall("\\d+\s*(?=AS _lookback_range_end_ts)", query)
        return (
            list(map(lambda indicator_id: int(indicator_id), indicator_list)),
            int(evaluation_time[0]) * 1000,
        )

    def build(
        self,
        computed_dataset_query: str,
        calculation_indicator_id_pairs: List[Tuple[str, str]],
        group_fields: List[str],
        timestamp_field: str,
        evaluation_time: int,
        lookback_time: int,
        filter_str: str,
        name_data_map: Dict = None,
        raw_dataset_names: Dict = None,
    ):
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

        computed_dataset_query = (
            parse_one(computed_dataset_query, read="postgres")
            .from_("df")
            .sql(dialect="duckdb")
        )
        # Temporary fix: Postgres to DuckDB translation for date format with "T"
        computed_dataset_query = computed_dataset_query.replace('"T"', "T")
        query = f"SELECT {','.join(select_predicate)} FROM ({computed_dataset_query})"
        query = f"{query} {where_predicate} {group_predicate}"
        query = parse_one(query, read="postgres").sql(dialect="duckdb")
        # Temporary fix: For datetime datatype
        datetime_col = lariat_string_utils.get_next_word(query, "EPOCH FROM")
        if datetime_col:
            query = query.replace(
                f"EPOCH FROM {datetime_col}",
                f"EPOCH FROM TRY_CAST({datetime_col} AS DATE)",
            )

        # Remove leading and trailing whitespaces for the sake of cleaner testing
        return query.strip()

    def run(self, query, output_path):
        logging.debug(f"Running Query: {query}")
        logging.debug(f"Writing Query to: {output_path}")
        indicator_statuses = []
        output_df = []
        for _, data in self.name_data_map.items():
            df = pd.DataFrame([data])
            response = pandas_sql_utils.run_sql_query(query=query, df=df)
            if response is not None:
                output_df.append(response)
                indicator_statuses.extend(
                    self.construct_indicator_statuses_from_meta(
                        query=query, meta_dict={}
                    )
                )
        output_df = pd.concat(output_df)
        return output_df, indicator_statuses
