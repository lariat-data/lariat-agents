from abc import ABC, abstractmethod
from typing import List, Tuple
import re
from lariat_agents.constants import (
    RESULT_OUTPUT_RESULT_MIN_TS,
    RESULT_OUTPUT_RESULT_MAX_TS,
    RESULT_OUTPUT_LOOKBACK_RANGE_END_TS,
    RESULT_OUTPUT_LOOKBACK_RANGE_START_TS,
)


class BaseQueryBuilder(ABC):
    """
    This class provides the expected interface to:
    - Take a group of indicators and return the actual query run against a database or data warehouse
    - Retrieve the schema from the database or data warehouse

    The implementor has the responsibility of ensuring that the data source works with both sketches and non-sketch
    modes, or failing initializtion of the query builder if a mode isn't supported.
    """

    def __init__(self, query_builder_type: str, sketch_mode: bool = True):
        """
        :param query_builder_type: Name of the query builder type
        :param sketch_mode: boolean indicating whether sketches or raw values should be returned
        """
        self._query_builder_type = query_builder_type
        # TODO: Create a property that tracks sketch_mode
        self.sketch_mode = sketch_mode

    def __str__(self):
        return self._query_builder_type

    @abstractmethod
    def fill_in_expressions_without_sketch_objects(
        self, calculation: str, indicator_id: str, group_fields: str = None
    ):
        """
        Provides functionality to create a snowflake query from Lariat Indicators without using data sketches
        :param calculation:
        :param indicator_id:
        :param group_fields:
        :return: String in the format "calculation as _indicator_id" e.g. "COUNT(udid) as _indicator_1"
        """

    @abstractmethod
    def fill_in_expressions_with_sketch_objects(
        self, calculation: str, indicator_id: str, group_fields: str = None
    ):
        """
        Provides functionality to create a snowflake query from Lariat Indicators with data sketches
        :param calculation:
        :param indicator_id:
        :param group_fields:
        :return: String in the format "calculation as _indicator_id" e.g. "COUNT(udid) as _indicator_1"
        """

    @abstractmethod
    def run_schema_retrieval(
        self,
        table_schema: str,
        table_names: List[str],
        source_id: str,
        db_name: str = None,
    ):
        """
        Schema Retrieval Query. Queries the correct part of information schema and retrieves the schemas along with
        any relevant metadata (e.g. partitions in athena etc.)
        :param source_id:
        :param table_names:
        :param table_schema:
        :param db_name:
        :return:
        """

    @abstractmethod
    def build(
        self,
        computed_dataset_query: str,
        calculation_indicator_id_pairs: List[Tuple[str, str]],
        group_fields: List[str],
        timestamp_field: str,
        evaluation_time: int,
        lookback_time: int,
        filter_str: str,
    ):
        """
        Constructs the query to collect data based on the provided calculations and indicators. This
        function is called for each compute family, i.e all indicators that can be grouped into one query.

        :param computed_dataset_query: the query to create the computed dataset
        :param calculation_indicator_id_pairs: List of tuple pairs of calculation string and indicator id
        :param group_fields: list of fields to group by
        :param timestamp_field: the field to use as the time dimension
        :param evaluation_time: the maximum time (according to timestamp_field) for which to pull data
        :param lookback_time: the amount of data to lookback for from evalaution time to compute the indicator
        :param filter_str: a string representing the sql predicate (from after the WHERE clause) to filter the data from the
        indicator
        :return: A string representation of a query to be executed to compute all of the indicators in the compute
        family
        """

    @abstractmethod
    def run(self, query, output_path):
        """
        Logic to execute the query against the data source (e.g. execute the actual athena query, snowflake query or
        read query a parquet file)
        :param query:
        :param output_path:
        :return:
        """
        pass

    @staticmethod
    def construct_select_predicate(field_description):
        """
        Return the selected predicate based on field description.
        If field description is a dict of type {'column': alias} then return the string "column as alias"
        If field description is just a string: col1 then return "col1 as col1"
        :param field_description: description of alias or just name of column
        :return: string representation of selection based on field description:
        Example 1: INPUT: {"col1": "alias"} OUTPUT: "col1 as alias"
        Example 2: INPUT: "col2" OUTPUT: "col2 as col2"
        """
        if type(field_description) is dict:
            if len(field_description) > 1:
                raise Exception
            else:
                for key, val in field_description.items():
                    key = key.strip()
                    val = val.strip()
                    return f"{key} as \"{val}\""
        elif type(field_description) is str:
            field_description = field_description.strip()
            return f'{field_description} as "{field_description}"'

    @staticmethod
    def add_timestamp_fields(
        timestamp_col: str, evaluation_time: int, lookback_time: int
    ):
        """
        Constructs part of the query to return the relevant timestamp metadata fields
        and the range of timestamp_col actually available in the inspected data for each sub group
        :param timestamp_col: timestamp column of interest in the query
        :param evaluation_time: time for which the evaluation occurs
        :param lookback_time: how much data to look back on
        :return: a string of the relevant timestamp queries e.g: MIN(received_time) as _result_min_ts,
                    MAX(received_time) as _result_max_ts, 1654646400 as _lookback_range_end_ts,
                    1654642800 as _lookback_range_start_ts
        """
        return (
            f"MIN({timestamp_col}) as {RESULT_OUTPUT_RESULT_MIN_TS}, MAX({timestamp_col}) as"
            f" {RESULT_OUTPUT_RESULT_MAX_TS}, {evaluation_time} as {RESULT_OUTPUT_LOOKBACK_RANGE_END_TS},"
            f" {evaluation_time - lookback_time} as {RESULT_OUTPUT_LOOKBACK_RANGE_START_TS}"
        )

    @staticmethod
    def construct_indicator_statuses_from_meta(query, meta_dict):
        indicator_statuses = []
        (
            indicator_list,
            evaluation_time,
        ) = BaseQueryBuilder.get_indicators_from_query_str(query)
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
        indicator_list = re.findall("as _indicator_(\\d+)", query)
        evaluation_time = re.findall("\\d+\s*(?=as _lookback_range_end_ts)", query)
        return (
            list(map(lambda indicator_id: int(indicator_id), indicator_list)),
            int(evaluation_time[0]) * 1000,
        )
