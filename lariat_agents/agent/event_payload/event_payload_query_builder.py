from typing import List, Dict

import pandas as pd
import fsspec
import pyarrow.parquet as pq
import pytz

from lariat_agents.agent.event_payload.event_payload_types import SupportedPayloadFormat
from lariat_agents.agent.event_payload.event_payload_utils import is_content_too_large

from datetime import datetime

from lariat_agents.base.streaming_base.streaming_base_query_builder import (
    StreamingBaseQueryBuilder,
)
import logging
import re
from lariat_python_common.sql.fields_uniquifier import DelimiterSeparatedListUniquifier
from shapely import wkb


CATEGORICAL_TYPE = "categorical"
NUMERICAL_TYPE = "numeric"
TIME_MAX_PREFIX = "max_"
TIME_MIN_PREFIX = "min_"
PREFIX_FOR_PRIMARY = TIME_MAX_PREFIX

META_PREFIX = "partition."
OBJECT_PREFIX = "object."
RESERVED_DIMENSIONS = ["scalar", "object", "list", "wkb"]


class EventPayloadQueryBuilder(StreamingBaseQueryBuilder):
    def __init__(
        self,
        query_builder_type: str,
        sketch_mode: bool = True,
        name_data_map: Dict = None,
        raw_dataset_names: List = None,
    ):
        self.name_data_map = name_data_map
        self.raw_dataset_names = raw_dataset_names
        super().__init__(query_builder_type=query_builder_type, sketch_mode=sketch_mode)

    @staticmethod
    def format_row(row, columns_and_separators):
        """
        Helper to take the template format in the config and pull out
        variables.
        :param row:
        :param columns_and_separators:
        :return:
        """
        components = []
        for col, sep in columns_and_separators:
            col_name = col.strip("{}")
            components.append(str(row[col_name]) + sep)
        return "".join(components)

    @staticmethod
    def adjust_date_precision(df, format_str, column, max_or_min="min"):
        # Determine the precision by checking if time components are present
        has_year = "%Y" in format_str
        has_month = "%m" in format_str or "%b" in format_str or "%B" in format_str
        has_day = "%d" in format_str
        has_hour = "%H" in format_str or "%I" in format_str
        has_minute = "%M" in format_str
        has_second = "%S" in format_str
        if max_or_min == "min":
            if has_second:  # Precision includes seconds
                return df[column]  # No adjustment needed
            elif has_minute:  # Precision includes minutes
                return df[column].dt.floor("min")
            elif has_hour:  # Precision includes hours
                return df[column].dt.floor("H")
            elif has_day:  # Precision is to the day
                return df[column].dt.floor("D")
            elif has_month:  # Precision is to the month
                return df[column].dt.to_period("M").dt.to_timestamp()
            elif has_year:
                return df[column].dt.to_period("Y").dt.to_timestamp()

        elif max_or_min == "max":
            if has_second:  # Precision includes seconds
                return df[column]  # No adjustment needed
            elif has_minute:  # Precision includes minutes
                return df[column].dt.floor("min") + pd.Timedelta(seconds=59)
            elif has_hour:  # Precision includes hours
                return df[column].dt.floor("H") + pd.Timedelta(minutes=59, seconds=59)
            elif has_day:  # Precision is to the day
                return df[column].dt.floor("D") + pd.Timedelta(
                    hours=23, minutes=59, seconds=59
                )
            elif has_month:  # Precision is to the month
                return (
                    df[column].dt.to_period("M").dt.to_timestamp()
                    + pd.offsets.MonthEnd(0)
                    + pd.Timedelta(hours=23, minutes=59, seconds=59)
                )
            elif has_year:
                return (
                    df[column].dt.to_period("Y").dt.to_timestamp()
                    + pd.offsets.YearEnd(0)
                    + pd.Timedelta(hours=23, minutes=59, seconds=59)
                )

        return df[column]

    @staticmethod
    def adjust_datetime_precision(row, format_str, max_or_min="min"):
        # Determine the precision by checking if time components are present
        has_year = "%Y" in format_str
        has_month = "%m" in format_str or "%b" in format_str or "%B" in format_str
        has_day = "%d" in format_str
        has_hour = "%H" in format_str or "%I" in format_str
        has_minute = "%M" in format_str
        has_second = "%S" in format_str
        if max_or_min == "min":
            second = 0
            minute = 0
            hour = 0
            day = 1
            month = 1
        elif max_or_min == "max":
            second = 59
            minute = 59
            hour = 23
            if has_month:
                day = row + pd.offsets.MonthEnd(1)
            else:
                day = 31
            month = 12
        else:
            raise TypeError(
                f"Expected one of min or max in the max_or_min argument. Got {max_or_min}"
            )
        # Adjust based on detected precision
        if has_second:  # Precision includes seconds
            return row  # No adjustment needed
        elif has_minute:  # Precision includes minutes
            return row.replace(second=second)
        elif has_hour:  # Precision includes hours
            return row.replace(minute=minute, second=second)
        elif has_day:  # Precision is to the day
            return row.replace(hour=hour, minute=minute, second=second)
        elif has_month:  # Precision is to the month
            return row.replace(day=day, hour=hour, minute=minute, second=second)
        elif has_year:
            return row.replace(
                month=month, day=day, hour=hour, minute=minute, second=second
            )
        else:
            return row

    @staticmethod
    def get_filtered_columns(available_columns, selected_columns):
        filtered_list = []
        for item in selected_columns:
            if item in available_columns:
                filtered_list.append(item)
            else:
                logging.warning(f"Selected Configure column {item} is not in dataset")
        return filtered_list

    @staticmethod
    def set_df_timestamp_vars(df, timestamp_mappings):
        timestamp_cols = []
        primary_timestamp_column = None
        first_timestamp_column = None
        for key in timestamp_mappings:
            column = timestamp_mappings[key]["column"]
            format_str = timestamp_mappings[key]["format"]
            timezone = timestamp_mappings[key].get("timezone", "")
            is_primary = timestamp_mappings[key].get("primary", False)
            columns_and_separators = re.findall(r"(\{[^}]+\})([^{]*)", column)
            if columns_and_separators:
                col_list = [col[0].strip("{}") for col in columns_and_separators]
                filter_list = EventPayloadQueryBuilder.get_filtered_columns(
                    df, col_list
                )

                if filter_list == col_list:
                    for col, sep in columns_and_separators:
                        col_name = col.strip("{}")
                        if key in df.columns:
                            df[key] = df[key] + df[col_name] + sep
                        else:
                            df[key] = df[col_name] + sep
                    if first_timestamp_column is None:
                        first_timestamp_column = f"{PREFIX_FOR_PRIMARY}{key}"
                    if is_primary:
                        if primary_timestamp_column is None:
                            primary_timestamp_column = f"{PREFIX_FOR_PRIMARY}{key}"
                        else:
                            logging.warning(
                                f"Multiple primary timestamps defined. Staying with the first option: "
                                f"{primary_timestamp_column.removeprefix(PREFIX_FOR_PRIMARY)}"
                            )
                else:
                    logging.warning(
                        f"One of the chosen timestamp fields isn't in the data. Couldn't create {key}"
                    )
                    continue
            else:
                filter_list = EventPayloadQueryBuilder.get_filtered_columns(
                    df, [column]
                )
                if filter_list:
                    df[key] = df[column]
                    if first_timestamp_column is None:
                        first_timestamp_column = f"{PREFIX_FOR_PRIMARY}{key}"
                    if is_primary:
                        if primary_timestamp_column is None:
                            primary_timestamp_column = f"{PREFIX_FOR_PRIMARY}{key}"
                        else:
                            logging.warning(
                                f"Multiple primary timestamps defined. Staying with the first option: "
                                f"{primary_timestamp_column.removeprefix(PREFIX_FOR_PRIMARY)}"
                            )
                else:
                    logging.warning(
                        f"One of the chosen timestamp fields isn't in the data. Couldn't create {key}"
                    )
                    continue
            if format_str != "unixtime":
                df[key] = df[key].astype("string")
                df[key] = df[key].str.strip()
                df[key] = pd.to_datetime(df[key], format=format_str)
                if timezone:
                    try:
                        if df[key].dt.tz is None:
                            df[key] = df[key].dt.tz_localize(timezone)
                        else:
                            df[key] = df[key].dt.tz_convert(timezone)
                    except Exception as e:
                        logging.warning("Couldn't find timezone, keeping UTC")
                        if df[key].dt.tz is None:
                            df[key] = df[key].dt.tz_localize("UTC")
                else:
                    if df[key].dt.tz is None:
                        df[key] = df[key].dt.tz_localize("UTC")
                    else:
                        if df[key].dt.tz != pytz.UTC:
                            df[key] = df[key].dt.tz_convert("UTC")

                df[
                    f"{TIME_MAX_PREFIX}{key}"
                ] = EventPayloadQueryBuilder.adjust_date_precision(
                    df, format_str, key, "max"
                )
                df[
                    f"{TIME_MIN_PREFIX}{key}"
                ] = EventPayloadQueryBuilder.adjust_date_precision(
                    df, format_str, key, "min"
                )

                df[f"{TIME_MAX_PREFIX}{key}"] = (
                    df[f"{TIME_MAX_PREFIX}{key}"].astype("int64") // 10**9
                )
                df[f"{TIME_MIN_PREFIX}{key}"] = (
                    df[f"{TIME_MIN_PREFIX}{key}"].astype("int64") // 10**9
                )

            else:
                if timezone:
                    logging.warning(
                        f"Time is already in unix time. The timezone configuration {timezone} is ignored."
                    )
                df[key] = df[key].astype(int)
                # These columns are only added in for code reuse
                df[f"{TIME_MAX_PREFIX}{key}"] = df[key]
                df[f"{TIME_MIN_PREFIX}{key}"] = df[key]

            timestamp_cols.extend(
                [f"{TIME_MAX_PREFIX}{key}", f"{TIME_MIN_PREFIX}{key}"]
            )

        if primary_timestamp_column is None:
            if first_timestamp_column is not None:
                primary_timestamp_column = first_timestamp_column
                logging.info(
                    f"No Timestamp Column Set. Assigning it to the "
                    f"first encountered timestamp column "
                    f"{primary_timestamp_column.removeprefix(PREFIX_FOR_PRIMARY)}"
                )
        return timestamp_cols, primary_timestamp_column

    @staticmethod
    def calculate_aggregations(
        df,
        columns,
        dimensions,
        timestamp_cols,
        source_id,
        dataset_name,
        unfiltered_dimensions,
        aggregations=None,
    ):
        agg_dict = {}
        if not columns or not timestamp_cols:
            return None
        for agg in aggregations:
            for col in columns:
                if col in agg_dict:
                    agg_dict[col].append(agg)
                else:
                    agg_dict[col] = [agg]
        for col in timestamp_cols:
            if col.startswith("max"):
                if col in agg_dict:
                    agg_dict[col].append("max")
                else:
                    agg_dict[col] = ["max"]
            if col.startswith("min"):
                if col in agg_dict:
                    agg_dict[col].append("min")
                else:
                    agg_dict[col] = ["min"]

        if dimensions:
            grouped = df.groupby(dimensions, dropna=False)
            grouped = grouped.agg(agg_dict).reset_index()
        else:
            # If ungrouped - still mimic the format that comes from running agg on grouped dataframe
            results = []
            # Have to separate out based on function type because lambda operations don't work in non-grouped dfs
            for col, funcs in agg_dict.items():
                col_results = []
                for func in funcs:
                    if isinstance(func, str):
                        result = df[col].agg(func)
                        col_results.append((col, func, result))
                    elif isinstance(func, tuple):
                        # Handle lambda ops
                        func_name, func_lambda = func
                        result = func_lambda(df[col])
                        col_results.append((col, func_name, result))

                results.extend(col_results)
            results_df = pd.DataFrame(
                [{(col, func): value for col, func, value in results}], index=[0]
            )

            results_df.columns = pd.MultiIndex.from_tuples(results_df.columns)
            grouped = results_df

        derived_metric_key_tail = (
            f"{'|'.join(unfiltered_dimensions)}|{source_id}|{dataset_name}"
        )
        list_uniquifier = DelimiterSeparatedListUniquifier("|")
        hashed_key_tail = list_uniquifier.uniquify_string(derived_metric_key_tail)
        grouped.columns = [
            f"{col[1]}|{col[0]}|{hashed_key_tail}"
            if col[0] in columns and col[1]
            else col[0]
            for col in grouped.columns
        ]
        return grouped

    @staticmethod
    def resolve_duplicate_columns_from_columns_list(
        object_keys,
        partition_keys,
        column_list,
    ):
        common_keys = list(set(object_keys) & set(partition_keys))
        keys_to_keep_in_objects = set()
        new_column_list = []
        column_transforms = {}
        for mapping in column_list:
            if (
                mapping.startswith(OBJECT_PREFIX)
                and mapping.removeprefix(OBJECT_PREFIX) in object_keys
            ):
                if mapping.removeprefix(OBJECT_PREFIX) in common_keys:
                    keys_to_keep_in_objects.add(mapping)
                else:
                    mapping = mapping.removeprefix(OBJECT_PREFIX)
                new_column_list.append(mapping)
            elif (
                mapping.startswith(META_PREFIX)
                and mapping.removeprefix(META_PREFIX) in partition_keys
            ):
                if mapping.removeprefix(META_PREFIX) in common_keys:
                    keys_to_keep_in_objects.add(mapping)
                else:
                    mapping = mapping.removeprefix(META_PREFIX)
                new_column_list.append(mapping)
            elif mapping in common_keys:
                new_column_list.append(f"{META_PREFIX}{mapping}")
                column_transforms[mapping] = f"{META_PREFIX}{mapping}"
            else:
                new_column_list.append(mapping)

        return new_column_list, keys_to_keep_in_objects, column_transforms

    @staticmethod
    def handle_column_naming_in_df(
        df,
        object_keys,
        partition_keys,
        partition_fields_in_data,
        dimensions,
        string_columns,
        numeric_columns,
    ):
        """
        When we have overlaps in column names in partitions and in the file content itself, we handle it by
        adding the appropriate prefixes to the data. We also remove unnecessary prefixes when not necessary.

        Additionally, we fold in the partition_fields into the dataframe
        :param df:
        :param object_keys:
        :param partition_keys:
        :param partition_fields_in_data:
        :param dimensions:
        :param string_columns:
        :param numeric_columns:
        :return:
        """
        (
            dimensions,
            keys_to_keep_in_objects,
            dim_column_transforms,
        ) = EventPayloadQueryBuilder.resolve_duplicate_columns_from_columns_list(
            object_keys, partition_keys, dimensions
        )
        (
            string_columns,
            string_keys_to_keep,
            str_column_transforms,
        ) = EventPayloadQueryBuilder.resolve_duplicate_columns_from_columns_list(
            object_keys, partition_keys, string_columns
        )
        (
            numeric_columns,
            numeric_keys_to_keep,
            num_column_transforms,
        ) = EventPayloadQueryBuilder.resolve_duplicate_columns_from_columns_list(
            object_keys, partition_keys, numeric_columns
        )
        keys_to_keep_in_objects = (
            keys_to_keep_in_objects | numeric_keys_to_keep | string_keys_to_keep
        )
        rename_dict = {
            col.removeprefix(META_PREFIX).removeprefix(OBJECT_PREFIX): col
            for col in keys_to_keep_in_objects
        }
        df.rename(columns=rename_dict, inplace=True)
        partition_field_mappings = {
            **dim_column_transforms,
            **str_column_transforms,
            **num_column_transforms,
        }
        if partition_fields_in_data:
            for key, value in partition_fields_in_data.items():
                if key in partition_field_mappings:
                    df[partition_field_mappings[key]] = value
                else:
                    df[key] = value
        return df, dimensions, string_columns, numeric_columns

    @staticmethod
    def setup_dimensions(df, dimensions):
        new_dimensions = []
        # When the dimension isn't in the RESERVED_DIMENSION list, it is treated as a regular scalar dimension
        for dim in dimensions:
            if dim not in RESERVED_DIMENSIONS:
                new_dimensions.append(dim)

        # Handle specified dimension cases such as geometries, maps and scalars
        if "scalar" in dimensions:
            for dim in dimensions["scalar"]:
                new_dimensions.append(dim)
        if "object" in dimensions:
            for dim in dimensions["object"]:
                sin, gim = dim.split(".")
                df[dim] = df[sin].apply(
                    lambda x: x.get(gim, "") if isinstance(x, dict) else ""
                )
                new_dimensions.append(dim)
        if "list" in dimensions:
            for dim in dimensions["list"]:
                for name in dim:
                    idx = dim[name]["index"]
                    col = dim[name]["col"]
                    for sin in col:
                        df[name] = df[sin].apply(
                            lambda x: x[idx].get(col.get(sin), "")
                            if isinstance(x[idx], dict)
                            else ""
                        )
                    new_dimensions.append(name)

        if "wkb" in dimensions:
            for dim in dimensions["wkb"]:
                df[dim] = df[dim].apply(lambda x: wkb.loads(x).geom_type)
                new_dimensions.append(dim)

        return df, new_dimensions

    def handle_calculation(
        self,
        df,
        partition_fields_in_data,
        dimensions,
        string_columns,
        numeric_columns,
        timestamp_mappings,
        source_id,
        dataset_name,
    ):
        object_keys = df.columns
        partition_keys = list()
        df, dimensions = self.setup_dimensions(df, dimensions)
        if partition_fields_in_data:
            partition_keys = list(partition_fields_in_data.keys())
        (
            df,
            dimensions,
            string_columns,
            numeric_columns,
        ) = EventPayloadQueryBuilder.handle_column_naming_in_df(
            df,
            object_keys,
            partition_keys,
            partition_fields_in_data,
            dimensions,
            string_columns,
            numeric_columns,
        )
        columns_set = set(df.columns)
        timestamp_cols, primary_timestamp_column = self.set_df_timestamp_vars(
            df, timestamp_mappings
        )

        string_columns = self.get_filtered_columns(columns_set, string_columns)
        numeric_columns = self.get_filtered_columns(columns_set, numeric_columns)
        filtered_dimensions = self.get_filtered_columns(columns_set, dimensions)
        total_record_count = df.shape[0]

        if timestamp_cols and not (not filtered_dimensions and len(dimensions) >= 1):
            output_data_string_agg = self.calculate_aggregations(
                df,
                string_columns,
                filtered_dimensions,
                timestamp_cols,
                source_id,
                dataset_name,
                dimensions,
                ["count", ("null_count", lambda x: x.isnull().sum())],
            )
            temp_df = df[numeric_columns].apply(pd.to_numeric, errors="coerce")
            filtered_numeric_columns = []
            for col in numeric_columns:
                if temp_df[col].isna().all():
                    logging.warning(
                        f"Column '{col}' cannot be converted to numeric type."
                    )
                else:
                    filtered_numeric_columns.append(col)
            df[filtered_numeric_columns] = df[filtered_numeric_columns].apply(
                pd.to_numeric, errors="coerce"
            )
            output_data_numeric_agg = self.calculate_aggregations(
                df,
                filtered_numeric_columns,
                filtered_dimensions,
                timestamp_cols,
                source_id,
                dataset_name,
                dimensions,
                [
                    "count",
                    "sum",
                    "max",
                    "min",
                    "mean",
                    "std",
                    ("null_count", lambda x: x.isnull().sum()),
                ],
            )

            if (
                output_data_numeric_agg is not None
                and output_data_string_agg is not None
            ):
                merged_df = pd.merge(
                    output_data_string_agg,
                    output_data_numeric_agg,
                    on=filtered_dimensions + timestamp_cols,
                    how="inner",
                )

            elif output_data_numeric_agg is None and output_data_string_agg is not None:
                merged_df = output_data_string_agg
            elif output_data_numeric_agg is not None and output_data_string_agg is None:
                merged_df = output_data_numeric_agg
            else:
                merged_df = None
        else:
            merged_df = None

        merged_df[dimensions] = merged_df[dimensions].apply(
            lambda x: x.str.strip().replace("", "<empty>").fillna("<empty>")
        )

        return (
            merged_df,
            total_record_count,
            primary_timestamp_column,
            timestamp_cols,
            filtered_dimensions,
            dimensions,
        )

    @staticmethod
    def add_metadata_to_results(
        merged_df,
        dimensions,
        primary_timestamp_column,
        timestamp_cols,
        total_record_count,
        execution_time,
        location_info,
    ):
        merged_df["total_record_count"] = total_record_count
        merged_df["lariat_agent_execution_time"] = execution_time
        if isinstance(location_info, tuple) and len(location_info) == 2:
            merged_df["bucket"] = location_info[0]
            merged_df["object"] = location_info[1]

        merged_df.rename(
            columns={
                col: f"dim|{col}" for col in dimensions if col in merged_df.columns
            },
            inplace=True,
        )
        merged_df.rename(
            columns={
                col: f"time|{col}"
                if col != primary_timestamp_column
                else f"primary_time|{col}"
                for col in timestamp_cols
                if col in merged_df.columns
            },
            inplace=True,
        )
        return merged_df

    @staticmethod
    def combine_streaming_results(merged_df, dimensions, timestamp_cols):
        agg_dict = {}
        columns_to_add_in = []
        for col in merged_df.columns:
            if col not in dimensions and col not in timestamp_cols:
                if (
                    col.startswith("count|")
                    or col.startswith("sum|")
                    or col.startswith("null_count|")
                ):
                    agg_dict[col] = "sum"
                elif col.startswith("mean|"):
                    columns_to_add_in.append(col)
                elif col.startswith("max|"):
                    agg_dict[col] = "max"
                elif col.startswith("min|"):
                    agg_dict[col] = "min"

        for col in timestamp_cols:
            if col.startswith("max"):
                agg_dict[col] = ["max"]
            if col.startswith("min"):
                agg_dict[col] = ["min"]

        grouped = merged_df.groupby(dimensions, dropna=False)
        grouped = grouped.agg(agg_dict).reset_index()
        grouped.columns = [col[0] for col in grouped.columns]
        for col in columns_to_add_in:
            if col.startswith("mean|"):
                grouped[col] = (
                    grouped[f"sum|{col.removeprefix('mean|')}"]
                    / grouped[f"count|{col.removeprefix('mean|')}"]
                )
        return grouped

    def run_streaming(
        self,
        fsspec_name,
        partition_fields_in_data,
        file_type,
        string_columns,
        numeric_columns,
        timestamp_mappings,
        dimensions,
        source_id,
        dataset_name,
        location_info,
        execution_time,
    ):
        chunksize = 200000
        pq_file_handler = None
        chunks = None
        if file_type == SupportedPayloadFormat.JSONL:
            chunks = pd.read_json(fsspec_name, lines=True, chunksize=chunksize)
            logging.info(f"Chunked Data into {chunksize} chunks")
        elif file_type == SupportedPayloadFormat.JSON:
            chunks = pd.read_json(fsspec_name, chunksize=chunksize)
            logging.info(f"Chunked Data into {chunksize} chunks")
        elif file_type == SupportedPayloadFormat.PARQUET:
            pq_file_handler = fsspec.open(fsspec_name).open()
            parquet_file = pq.ParquetFile(pq_file_handler)
            chunks = (
                parquet_file.read_row_group(i)
                for i in range(parquet_file.num_row_groups)
            )
            logging.info(f"Chunked Data into {parquet_file.num_row_groups} chunks")
        elif file_type == SupportedPayloadFormat.CSV:
            chunks = pd.read_csv(fsspec_name, on_bad_lines="skip", chunksize=chunksize)
            logging.info(f"Chunked Data into {chunksize} chunks")

        merged_df = None
        primary_timestamp_column = None
        timestamp_cols = None
        total_record_count = 0
        filtered_dimensions = None
        chunk_results = []

        if chunks is not None:
            for chunk in chunks:
                if file_type == SupportedPayloadFormat.PARQUET:
                    chunk = chunk.to_pandas()
                (
                    chunked_df,
                    chunk_record_count,
                    primary_timestamp_column,
                    timestamp_cols,
                    filtered_dimensions,
                    final_dimensions,
                ) = self.handle_calculation(
                    chunk,
                    partition_fields_in_data,
                    dimensions,
                    string_columns,
                    numeric_columns,
                    timestamp_mappings,
                    source_id,
                    dataset_name,
                )
                total_record_count += chunk_record_count

                if chunked_df is not None:
                    chunked_df = chunked_df.where(pd.notnull(chunked_df), None)
                    chunk_results.append(chunked_df)
            if pq_file_handler is not None:
                pq_file_handler.close()
            if chunk_results:
                merged_df = pd.concat(chunk_results)
            if merged_df is not None:
                merged_df = self.combine_streaming_results(
                    merged_df, final_dimensions, timestamp_cols
                )
                merged_df = self.add_metadata_to_results(
                    merged_df,
                    final_dimensions,
                    primary_timestamp_column,
                    timestamp_cols,
                    total_record_count,
                    execution_time,
                    location_info,
                )
                return (
                    merged_df,
                    execution_time,
                    f"primary_time|{primary_timestamp_column}",
                    filtered_dimensions,
                )
        if pq_file_handler is not None:
            pq_file_handler.close()
        return None, execution_time, None, None

    def run_batch(
        self,
        fsspec_name,
        partition_fields_in_data,
        file_type,
        string_columns,
        numeric_columns,
        timestamp_mappings,
        dimensions,
        source_id,
        dataset_name,
        location_info,
        execution_time,
    ):
        df = None
        if file_type == SupportedPayloadFormat.JSONL:
            df = pd.read_json(fsspec_name, lines=True)
        elif file_type == SupportedPayloadFormat.JSON:
            df = pd.read_json(fsspec_name)
        elif file_type == SupportedPayloadFormat.PARQUET:
            df = pd.read_parquet(fsspec_name)
        elif file_type == SupportedPayloadFormat.CSV:
            df = pd.read_csv(fsspec_name, on_bad_lines="skip")
        if df is not None:
            (
                merged_df,
                total_record_count,
                primary_timestamp_column,
                timestamp_cols,
                filtered_dimensions,
                final_dimensions,
            ) = self.handle_calculation(
                df,
                partition_fields_in_data,
                dimensions,
                string_columns,
                numeric_columns,
                timestamp_mappings,
                source_id,
                dataset_name,
            )
            if merged_df is not None:
                merged_df = self.add_metadata_to_results(
                    merged_df,
                    final_dimensions,
                    primary_timestamp_column,
                    timestamp_cols,
                    total_record_count,
                    execution_time,
                    location_info,
                )
            return (
                merged_df,
                execution_time,
                f"primary_time|{primary_timestamp_column}",
                filtered_dimensions,
            )
        else:
            return None, execution_time, None, None

    def run(
        self,
        fsspec_name,
        partition_fields_in_data,
        clean_schema,
        file_type,
        string_columns,
        numeric_columns,
        timestamp_mappings,
        dimensions,
        source_id,
        dataset_name,
        location_info,
        content_length,
    ):
        execution_time = int(datetime.now().timestamp())
        should_stream = is_content_too_large(content_length)
        if not should_stream:
            return self.run_batch(
                fsspec_name,
                partition_fields_in_data,
                file_type,
                string_columns,
                numeric_columns,
                timestamp_mappings,
                dimensions,
                source_id,
                dataset_name,
                location_info,
                execution_time,
            )
        else:
            return self.run_streaming(
                fsspec_name,
                partition_fields_in_data,
                file_type,
                string_columns,
                numeric_columns,
                timestamp_mappings,
                dimensions,
                source_id,
                dataset_name,
                location_info,
                execution_time,
            )
