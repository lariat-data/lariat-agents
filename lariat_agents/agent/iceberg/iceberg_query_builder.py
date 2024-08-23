from lariat_agents.base.batch_base.batch_base_query_builder import BatchBaseQueryBuilder
import time
import lariat_python_common.sql.utils as lariat_sql_utils
import sqlparse
from sqlparse.sql import Identifier, IdentifierList
import logging
import lariat_python_common.schema.utils as lariat_schema_utils
import lariat_python_common.string.utils as lariat_string_utils
import lariat_python_common.pandas.utils as pandas_sql_utils
from lariat_agents.constants import (
    LARIAT_EVENT_NAME,
    ICEBERG_STRICT_ENFORCE_PARTITIONS,
    MAX_ICEBERG_FILES,
)
import json
from typing import List, Dict
import datetime
from datetime import timezone
from pyiceberg.types import (
    StringType,
    LongType,
    TimestampType,
    TimestamptzType,
    DateType,
)
from sqlglot import parse_one, exp
from sqlglot.expressions import replace_tables


class IcebergQueryBuilder(BatchBaseQueryBuilder):
    def __init__(
        self,
        query_builder_type: str,
        sketch_mode: bool = True,
    ):
        super().__init__(query_builder_type=query_builder_type, sketch_mode=sketch_mode)
        self.dataset_to_catalog_map = None
        self.catalogs = None

    def run_schema_retrieval(
        self,
        table_schema: str,
        table_names: List[str],
        source_id: str,
        db_name: str = None,
        catalog=None,
    ):
        schema_output = []
        if not catalog:
            raise ValueError("Cannot find schema without a valid catalog")

        for table_name in table_names:
            try:
                qualified_table_name = f"{table_schema}.{table_name}"
                if catalog.table_exists(qualified_table_name):
                    table = catalog.load_table(qualified_table_name)
                    arrow_schema = table.schema().as_arrow()
                    output_json_schema = (
                        lariat_schema_utils.convert_arrow_schema_to_json_schema(
                            arrow_schema
                        )
                    )
                    schema_output.append(
                        {
                            "schema": json.loads(output_json_schema),
                            "raw_dataset_name": f"{catalog.name}.{table_schema}.{table_name}",
                            "raw_dataset_data_source": self._query_builder_type,
                            "raw_dataset_source_id": source_id,
                            "raw_dataset_event_name": LARIAT_EVENT_NAME,
                            "meta": {
                                "last_partition_id": table.last_partition_id(),
                                "current_snapshot": table.current_snapshot().__repr__(),
                                "sort_orders": table.sort_orders().__repr__(),
                                "current_schema_id": table.metadata.current_schema_id,
                                "partition_spec": table.spec().__repr__(),
                                "specs_struct": table.metadata.specs_struct().__repr__(),
                            },
                        }
                    )
                else:
                    logging.warning(f"Table {qualified_table_name} doesn't exist")
            except Exception as e:
                logging.error(f"Failed to transform schema for: {table_name} {e}")

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
                resolved_calculation = "COUNT(DISTINCT {})".format(
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

    @staticmethod
    def get_time_intervals(unix_timestamp):
        dt_object = datetime.datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
        # Extract the year, month, day, hour, minute, and second
        year = dt_object.year
        month = "{:02d}".format(dt_object.month)
        day = "{:02d}".format(dt_object.day)
        hour = "{:02d}".format(dt_object.hour)
        minute = "{:02d}".format(dt_object.minute)
        second = "{:02d}".format(dt_object.second)
        return year, month, day, hour, minute, second

    def get_iceberg_scan_filter(
        self,
        table,
        timestamp_field: str,
        evaluation_time: int,
        lookback_time: int,
        filter_str: str,
        computed_dataset_query,
    ):
        partition_fields = table.spec()
        parsed_computed_dataset_query = sqlparse.parse(computed_dataset_query)[0]
        time_partition_list = []
        time_non_partition_list = []
        column_names = table.schema().column_names
        is_time_col_underived = False
        if timestamp_field in column_names:
            timestamp_field_type = table.schema().find_type(timestamp_field)
        else:
            timestamp_field_type = LongType()
        for token in parsed_computed_dataset_query.tokens:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    if isinstance(identifier, Identifier):
                        alias = identifier.get_alias()
                        timestamp_calculation = (
                            str(identifier)
                            .lower()
                            .removesuffix(f"as {timestamp_field}")
                            .removesuffix(f'as "{timestamp_field}"')
                            .strip()
                        )
                        if alias is None:
                            alias = identifier.get_real_name()
                        if alias == timestamp_field:
                            if alias != timestamp_calculation:
                                timestamp_field_type = LongType()
                            else:
                                is_time_col_underived = True
                            if partition_fields.fields:
                                for key in partition_fields:
                                    if key.name in timestamp_calculation:
                                        time_partition_list.append(key)
                            for col in parse_one(timestamp_calculation).find_all(
                                exp.Column
                            ):
                                if col.name in column_names:
                                    time_non_partition_list.append(col.name)

        time_columns = time_partition_list + time_non_partition_list
        if not time_columns:
            if timestamp_field in column_names:
                is_time_col_underived = True
                time_columns.append(timestamp_field)
                if partition_fields.fields:
                    for key in partition_fields:
                        if key.name == timestamp_field:
                            time_partition_list.append(key)

        if not time_partition_list:
            if not ICEBERG_STRICT_ENFORCE_PARTITIONS:
                logging.warning(
                    "No Partition Fields in the Timestamp Filter. This can yield a very expensive query"
                )
            else:
                logging.error(
                    "No Partition Fields in the Timestamp Filter. This can yield a very expensive query"
                )
                raise ValueError(
                    "Unoptimized Indicator - Timestamp filter not from partitions"
                )

        (
            start_year,
            start_month,
            start_day,
            start_hour,
            start_minute,
            start_second,
        ) = self.get_time_intervals(evaluation_time - lookback_time)

        (
            end_year,
            end_month,
            end_day,
            end_hour,
            end_minute,
            end_second,
        ) = self.get_time_intervals(evaluation_time)
        if filter_str and filter_str.strip() != "":
            filters = f"{filter_str} AND"
        else:
            filters = ""
        for column in time_columns:
            # Currently, all year,month, day,hour,minute columns are treated as strings
            if "year" in column.lower():
                filters = f"{filters} ({column} >= '{start_year}' AND {column} < '{end_year}') AND "
            elif "month" in column.lower():
                filters = f"{filters} ({column} >= '{start_month}' AND {column} < '{end_month}') AND "
            elif "day" in column.lower():
                filters = f"{filters} ({column} >= '{start_day}' AND {column} < '{end_day}') AND "
            elif "hour" in column.lower():
                filters = f"{filters} ({column} >= '{start_hour}' AND {column} < '{end_hour}') AND "
            elif "minute" in column.lower():
                filters = f"{filters} ({column} >= '{start_minute}' AND {column} < '{end_minute}') AND "
            else:
                if (
                    isinstance(timestamp_field_type, StringType)
                    or isinstance(timestamp_field_type, TimestampType)
                    or isinstance(timestamp_field_type, TimestamptzType)
                ):
                    filters = (
                        f"({column} >= '{start_year}-{start_month}-{start_day}T{start_hour}:"
                        f"{start_minute}:{start_second}' AND "
                        f"{column} < '{end_year}-{end_month}-{end_day}"
                        f"T{end_hour}:{end_minute}:{end_second}') AND "
                    )
                elif isinstance(timestamp_field_type, LongType):
                    filters = (
                        f"({column} >= {evaluation_time - lookback_time} AND "
                        f" {column} < {evaluation_time}) AND"
                    )
                elif isinstance(timestamp_field_type, DateType):
                    filters = (
                        f"({column} >= '{start_year}-{start_month}-{start_day}' AND "
                        f"{column} < '{end_year}-{end_month}-{end_day}') AND "
                    )
            filters = filters.strip().removesuffix("AND")
            if filters:
                scan = table.scan(row_filter=filters)
            else:
                scan = table.scan()
            if len(scan.plan_files()) >= MAX_ICEBERG_FILES:
                logging.warning(
                    f"Greater than {MAX_ICEBERG_FILES} being processed. Re-consider indicator frequency"
                )
            return scan.to_pandas(), is_time_col_underived
        if filter_str and filter_str.strip() != "":
            scan = table.scan(row_filter=filter_str)
        else:
            scan = table.scan()
        return scan.to_pandas(), False

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

        if raw_dataset_names:
            raw_dataset_name = raw_dataset_names[0]
        else:
            logging.warning("No raw datasets passed through")
            raise ValueError("No raw datasets passed through to indicator endpoint")

        catalog_name, db_name, table_name = raw_dataset_name.split(".")
        catalog = self.catalogs[catalog_name]
        table = catalog.load_table(f"{db_name}.{table_name}")
        data_df, is_time_col_underived = self.get_iceberg_scan_filter(
            table,
            timestamp_field,
            evaluation_time,
            lookback_time,
            filter_str,
            computed_dataset_query,
        )
        if is_time_col_underived is None:
            is_time_col_underived = False
        if data_df is None:
            return "", None

        where_predicate = ""
        if timestamp_field:
            if evaluation_time is None:
                evaluation_time = round(time.time())
            if lookback_time is None:
                lookback_time = 0
            if is_time_col_underived:
                timestamp_field_type = table.schema().find_type(timestamp_field)
            else:
                timestamp_field_type = LongType()
            should_inspect = isinstance(timestamp_field_type, LongType)
            select_predicate.append(
                self.add_timestamp_fields(
                    timestamp_field,
                    evaluation_time,
                    lookback_time,
                    inspect_results=should_inspect,
                )
            )

        if filter_str:
            where_predicate = f"WHERE {filter_str}"
        group_predicate = ""
        if len(group_fields) > 0:
            group_predicate = f"GROUP BY {','.join(group_fields)}"
        computed_dataset_query = replace_tables(
            parse_one(computed_dataset_query, read="duckdb"),
            {f"{catalog_name}.{db_name}.{table_name}": "df"},
        ).sql(dialect="duckdb")
        # Temporary fix: Postgres to DuckDB translation for date format with "T"
        computed_dataset_query = computed_dataset_query.replace('"T"', "T")
        query = f"SELECT {','.join(select_predicate)} FROM ({computed_dataset_query})"
        query = f"{query} {where_predicate} {group_predicate}"
        query = parse_one(query, read="duckdb").sql(dialect="duckdb")
        # Temporary fix: For datetime datatype
        datetime_col = lariat_string_utils.get_next_word(query, "EPOCH FROM")
        if datetime_col:
            query = query.replace(
                f"EPOCH FROM {datetime_col}",
                f"EPOCH FROM TRY_CAST({datetime_col} AS DATE)",
            )
        return query.strip(), data_df

    def run(self, query, output_path):
        logging.debug(f"Running Query: {query}")
        logging.debug(f"Writing Query to: {output_path}")
        indicator_statuses = []
        output_df = None
        query, data_df = query
        if query:
            output_df = pandas_sql_utils.run_sql_query(query=query, df=data_df)
            if output_df is not None:
                indicator_statuses.extend(
                    self.construct_indicator_statuses_from_meta(
                        query=query, meta_dict={}
                    )
                )
        return output_df, indicator_statuses
