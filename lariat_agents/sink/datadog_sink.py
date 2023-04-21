import pandas as pd
from lariat_agents.constants import (
    RESULT_DF_RESERVED_FIELDS,
    RESULTS_DF_INDICATOR_COL_PREFIX,
    RESULT_OUTPUT_RESULT_MAX_TS,
    RESULT_OUTPUT_RESULT_MIN_TS,
    RESULT_OUTPUT_LOOKBACK_RANGE_END_TS,
    RESULT_OUTPUT_LOOKBACK_RANGE_START_TS,
)
from lariat_agents.base.base_sink import BaseSink
from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_series import MetricSeries
import logging
from typing import Dict

DEFAULT_TAGS = ["lariat"]
DEFAULT_DATA_LIMIT_SECONDS = 60 * 60  # 1 hour API Limit


class DatadogSink(BaseSink):
    """
    Responsible for writing data from the agent straight to Datadog.
    N.B: Sketch queries are sent as values so aggregations and merging cannot happen on them.
    E.g. Count Distincts by group1, group2 are sent as a value - so trying to get a distinct ungrouped cannot work with
    this sink option. Use the Lariat Sink for this.
    """

    def __init__(
        self,
        datadog_api_key: str,
        datadog_application_key: str,
        source_cloud: str,
        use_execution_time: bool = False,
        add_lag_time: bool = True,
    ):
        self.configuration = Configuration()
        self.configuration.api_key["apiKeyAuth"] = datadog_api_key
        self.configuration.api_key["appKeyAuth"] = datadog_application_key
        self.add_lag_time = add_lag_time
        self.use_execution_time = use_execution_time
        super().__init__(source_cloud=source_cloud, supports_tags=True)

    @staticmethod
    def _df_row_to_metric_series(row, metric_name, indicator_col, dimensions, tag_list):
        """
        Formats the value for a given timestamp to datadog. Converts Lariat results to a Datadog MetricSeries Type
        Dimensions are treated as tags of format dimension_col_name:dimension_col_value (e.g. COUNTRYCODE:USA)
        :param row: row represents a value for a given indicator and timestamp
        :param metric_name: The name associated with this metric
        :param indicator_col: Column within the dataframe of interest
        :param dimensions: Group & Filter fields to be added as tags
        :param tag_list: tags passed along by the agent to be added as datadog tags
        :return: Datadog MetricSeries
        """
        tags = [f"{dimension}:{row[dimension]}" for dimension in dimensions]
        tags.extend(DEFAULT_TAGS)
        tags.extend(tag_list)
        series = None
        try:
            series = MetricSeries(
                metric=metric_name,
                points=[
                    MetricPoint(
                        timestamp=int(row[RESULT_OUTPUT_LOOKBACK_RANGE_END_TS]),
                        value=float(row[indicator_col]),
                    )
                ],
                tags=tags,
            )
        except Exception as e:
            logging.warning(
                f"Failed to write indicator for {metric_name} to external source: {e.__traceback__}"
            )

        return series

    def write(
        self,
        result_df: pd.DataFrame = None,
        source_top_level: str = None,
        file_path: str = None,
        tags_dict: Dict = None,
    ):
        """
        Send Indicator results to Datadog as a MetricSeries
        TODO: Add in the functionality for reading the data from file_path and source_top_level
        """
        if (result_df is not None) and (not result_df.empty):
            # Filter all parts of the dataframe with a null max and min result ts
            result_df = result_df[~result_df[RESULT_OUTPUT_RESULT_MAX_TS].isnull()]
            result_df = result_df[~result_df[RESULT_OUTPUT_RESULT_MIN_TS].isnull()]
            if result_df.empty:
                logging.warning(
                    "No data available for indicators: No Indicators Written"
                )
                return

            dimensions = [
                col
                for col in result_df
                if not col.startswith(RESULTS_DF_INDICATOR_COL_PREFIX)
                and col not in RESULT_DF_RESERVED_FIELDS
            ]
            indicator_filter_columns = [
                col
                for col in result_df
                if col.startswith(RESULTS_DF_INDICATOR_COL_PREFIX)
            ]
            if len(indicator_filter_columns) == 0:
                logging.error("No Indicator Column found in result dataset")
                raise ValueError("No Indicator Columns found in result dataset")

            for indicator_col in indicator_filter_columns:
                result_df = result_df[~result_df[indicator_col].isnull()]
                if result_df.empty:
                    logging.warning(f"Indicator {indicator_col} is empty.")
                    continue
                metric_name = f"lariat.{indicator_col}"

                tag_key = int(
                    indicator_col.removeprefix(RESULTS_DF_INDICATOR_COL_PREFIX)
                )
                tags_for_indicator = tags_dict.get(tag_key, [])
                name_prefix = "name:"
                if any(tag.startswith(name_prefix) for tag in tags_for_indicator):
                    metric_name = [
                        f"lariat.{tag.removeprefix(name_prefix)}"
                        for tag in tags_for_indicator
                        if tag.startswith(name_prefix)
                    ][0]
                    tags_for_indicator = [
                        tag
                        for tag in tags_for_indicator
                        if not tag.startswith(name_prefix)
                    ]
                    tags_for_indicator.extend(f"indicator_id:{tag_key}")

                series_for_submission = list(
                    result_df.apply(
                        self._df_row_to_metric_series,
                        axis=1,
                        args=(
                            metric_name,
                            indicator_col,
                            dimensions,
                            tags_for_indicator,
                        ),
                    )
                )
                body = MetricPayload(series=series_for_submission)

                with ApiClient(self.configuration) as api_client:
                    api_instance = MetricsApi(api_client)
                    response = api_instance.submit_metrics(body=body)
                    logging.debug(f"Datadog response: {response}")

        else:
            logging.warning("Empty Result Set: No Indicators Written")
