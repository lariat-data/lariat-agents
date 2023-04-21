import os.path
from abc import ABC, abstractmethod
from typing import List, Dict
from lariat_agents.constants import (
    CLOUD_TYPE_AZURE,
    CLOUD_TYPE_AWS,
    CLOUD_TYPE_NONE,
    CLOUD_AGENT_CONFIG_PATH,
    AZURE_STORAGE_CONFIG_CONTAINER,
    LARIAT_INDICATOR_STATUS_URL,
    AZURE_STORAGE_CONFIG_CONNECTION_STRING,
    INDICATOR_PAYLOAD_EVALUATION_TIMES_COL,
    INDICATOR_PAYLOAD_COMPUTE_HASH_COL,
    INDICATOR_PAYLOAD_GROUP_FIELDS_COL,
    INDICATOR_PAYLOAD_LOOKBACK_WINDOW_LENGTH_COL,
    INDICATOR_PAYLOAD_TIMESTAMP_COLUMN_COL,
    INDICATOR_PAYLOAD_COMPUTED_DATASET_QUERY_COL,
    INDICATOR_PAYLOAD_COMPUTED_DATASET_ID_COL,
    INDICATOR_PAYLOAD_COMPUTED_DATASET_SOURCE_COL,
    INDICATOR_PAYLOAD_FILTERS_COL,
    INDICATOR_PAYLOAD_EVALUATION_INTERVAL_COL,
    INDICATOR_PAYLOAD_INDICATOR_ID_COL,
    INDICATOR_PAYLOAD_CALCULATION_COL,
    INDICATOR_PAYLOAD_SKETCH_TYPE,
    INDICATOR_PAYLOAD_INDICATOR_NAME_COL,
    INDICATOR_PAYLOAD_INDICATOR_RAW_DATASETS_COL,
    INDICATOR_PAYLOAD_LAG_COL,
    ORG_ID,
    TAG_FILESYSTEM_PREFIX,
    INDICATOR_QUERY_OUTPUT_KEY_PREFIX,
    AGENT_INTER_QUERY_GAP,
    SINK_TYPE,
    LARIAT_SINK_TYPE,
    DATADOG_SINK_TYPE,
    GRAFANA_SINK_TYPE,
    LARIAT_API_KEY,
    LARIAT_APPLICATION_KEY,
    SKETCH_TYPE_DISTINCT,
    SKETCH_TYPE_NONE,
    SKETCH_TYPE_DECILE,
    RESULT_DF_RESERVED_FIELDS,
    RESULTS_DF_INDICATOR_COL_PREFIX,
)
from ruamel.yaml import YAML
import boto3
from s3path import S3Path
from azure.storage.blob import (
    BlobServiceClient,
)
import json
from urllib import request, parse
from urllib.error import HTTPError
import logging
import pandas as pd
import time
import lariat_python_common.sql.utils as lariat_sql_utils
import datetime
from lariat_agents.base.base_query_builder import BaseQueryBuilder
from lariat_agents.sink.lariat_sink import LariatSink
from lariat_agents.sink.datadog_sink import DatadogSink
import sqlparse
from io import StringIO
from pathlib import Path
from lariat_python_common.io.utils import (
    read_stringio_from_cloud,
    write_stringio_to_cloud,
)


class BaseAgent(ABC):
    """
    The BaseAgent class is responsible for being the control center of the Lariat Agent.
    It is where the actions for retrieving and running indicators, along with getting schemata
    are done.
    Inheriting agents have to:
    - Provide a QueryBuilder to the constructor that is responsible for constructing the data source specific
    flavours of queries (e.g. The AthenaQueryBuilder transforms the indicator query to the Athena appropriate syntax)
    - Build retrieve_schema function to specify the correct syntax to retrieve the schema (e.g. correctly querying
    information schemas and pulling the correct metadata)
    - Build map_action_to_function - specify the keywords that trigger:
        i. Dispatching queries
        ii. Copying results (if async)
        iii. Pulling schemas
        iv. Any source specific work

    The Base Agent class already has the flow built in for pulling the correct indicators from the Lariat platform.
    It also has the logic to send metadata about indicator status back to Lariat.
    It also pulls in the yaml config that has additional specification to customize the agent. (E.g. if the sink is
    changed in the yaml config, that is automatically customized by the base agent).
    N.B.: When new sinks are added, the constructor of this class needs to be aware in order to make sure
    they send data to the correctly configured sink.
    """

    def __init__(
        self,
        agent_type: str,
        cloud: str,
        query_builder: BaseQueryBuilder,
        api_key: str = None,
        application_key: str = None,
    ):
        self._agent_type = agent_type

        # TODO: Refactor to use CloudTypeMode instead of string
        self._cloud = cloud

        # Setup Lariat Connection Variables
        if not api_key:
            self._api_key = LARIAT_API_KEY
        else:
            self._api_key = api_key
        if not application_key:
            self._application_key = LARIAT_APPLICATION_KEY
        else:
            self._application_key = application_key
        self.query_builder = query_builder
        self._cloud_agent_config_path = None

        # Setup Agent Configuration
        if self._cloud == CLOUD_TYPE_AWS:
            self._cloud_agent_config_path = CLOUD_AGENT_CONFIG_PATH
        elif self._cloud == CLOUD_TYPE_AZURE:
            self._cloud_agent_config_path = (
                CLOUD_AGENT_CONFIG_PATH,
                AZURE_STORAGE_CONFIG_CONTAINER,
                AZURE_STORAGE_CONFIG_CONNECTION_STRING,
            )
        elif self._cloud == CLOUD_TYPE_NONE:
            self._cloud_agent_config_path = CLOUD_AGENT_CONFIG_PATH
        self.yaml_config = self.get_yaml_config()

        # Setup Sink Configuration
        if SINK_TYPE in self.yaml_config:
            sink_type = list(self.yaml_config[SINK_TYPE].keys())[0]
            sink_args = self.yaml_config[SINK_TYPE][sink_type]
            if sink_type == LARIAT_SINK_TYPE:
                self.sink = LariatSink(source_cloud=self._cloud)
            elif sink_type == DATADOG_SINK_TYPE:
                self.query_builder.sketch_mode = False
                self.sink = DatadogSink(source_cloud=self._cloud, **sink_args)
            else:
                raise ValueError(f"Unsupported sink type: {sink_type}")
        else:
            self.sink = LariatSink(source_cloud=self._cloud)

    def __str__(self):
        return self._agent_type

    @abstractmethod
    def map_action_to_function(self, action, event_dict=None):
        """
        :param action: The action to be undertaken by the agent. This is received from an external source
        such as a lambda, cloud function or argo job
        :param event_dict: map containing key-values relevant to the given action.
        E.g.: a map containing the event response from AWS when a query success trigger is
        received
        """

    @abstractmethod
    def schema_retrieval(self):
        """
        Read the yaml config, retrieve schemas from the agent store.
        Register the schema with the Lariat endpoint
        """

    def get_yaml_config(self) -> Dict:
        """
        Return YAML Config based on path
        AWS: (fully_qualified_s3_object_path)
        AZURE: (blob_path, storage_container, storage_connection_string)
        LOCAL: (local_path_to_yaml)
        :return: dictionary representing the yaml file
        """
        yaml = YAML(typ="safe")
        if self._cloud == CLOUD_TYPE_AWS:
            s3_agent_path = self._cloud_agent_config_path
            s3_handler = boto3.client("s3")
            s3path_obj = S3Path(f"/{s3_agent_path}")
            response = s3_handler.get_object(
                Bucket=s3path_obj.bucket, Key=s3path_obj.key
            )
            agent_config = yaml.load(response["Body"])
        elif self._cloud == CLOUD_TYPE_AZURE:
            (
                azure_blob_path,
                azure_storage_container,
                azure_storage_connection_string,
            ) = self._cloud_agent_config_path
            blob_service_client = BlobServiceClient.from_connection_string(
                azure_storage_connection_string
            )
            container_client = blob_service_client.get_container_client(
                container=azure_storage_container
            )
            agent_config = yaml.load(
                container_client.download_blob(azure_blob_path).readall()
            )
        elif self._cloud == CLOUD_TYPE_NONE:
            with open(self._cloud_agent_config_path) as agent_config_file:
                agent_config = yaml.load(agent_config_file)
        else:
            raise ValueError(f"Unsupported cloud environment: {self._cloud}")
        return agent_config

    def send_payload_to_agent(self, payload, endpoint) -> bool:
        """
        Send the JSON payloads back to the approriate Lariat endpoint with the API Key and Application Key
        correctly defined
        :param payload: JSON representing the data that needs to be sent to the web service
        :param endpoint: fully qualifed url to which the data is sent
        :return: True on success and False on failure
        """
        data = json.dumps(payload)
        data = data.encode("utf-8")
        req = request.Request(endpoint, data=data)
        req.add_header("X-Lariat-Api-Key", self._api_key)
        req.add_header("X-Lariat-Application-Key", self._application_key)
        try:
            request.urlopen(req)
        except HTTPError as e:
            logging.error(f"Failed to reach ingest service: {endpoint} {e}")
            return False

        return True

    def get_lariat_indicator_json(self, indicator_url):
        """
        Sends a request to the Lariat Server to receive a list of indicators and evaluation times
        to collect health metrics for.
        :return: Pandas dataframe with list of indicators that each have a list of evaluation times and other
        relevant information such as name, group_fields
        """
        parameters = {"rawDatasetSource": self._agent_type}
        query_string = parse.urlencode(parameters)

        indicator_url = indicator_url + "?" + query_string
        req = request.Request(indicator_url)
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Lariat-Api-Key", self._api_key)
        req.add_header("X-Lariat-Application-Key", self._application_key)
        with request.urlopen(req) as url:
            data = pd.read_json(url.read().decode()).fillna("")
        return data

    def execute_indicators(
        self,
        indicators: pd.DataFrame,
        expect_results: bool,
        sketch_type_in_hash: bool = False,
    ):
        """
        Represents the logic to parse the indicators format that comes from the Lariat service.
        This function optimizes the queries, by grouping together indicator queries that can be run all at once.
        It delegates the actual creation of the query format to the passed in QueryBuilder and then runs it.
        If results are expected synchronously from execution, the write function is called to send results, if not
        this is handled by the implementing agent class to call the write function on an async trigger.

        :param indicators:
        :param expect_results: if True this waits for results synchronously and calls the sink.write function.
        If false, this doesn't call sink.write and it is left to the implementing agent to do this in the
        map_action_to_function when reacting to an async trigger for query completion.
        :param sketch_type_in_hash: This is generally safe to set to False.
        If true, this makes sure that sketch functions are all run as individual queries
        each other. This might be necessary in cases where custom aggregations have to be called.
        :return Nothing is returned from this function
        """

        source_id = self.yaml_config["source_id"]
        if indicators.empty:
            return True
        df = indicators.explode(INDICATOR_PAYLOAD_EVALUATION_TIMES_COL)
        df = df[df[INDICATOR_PAYLOAD_EVALUATION_TIMES_COL] != ""]
        if df.empty:
            logging.info("No valid evaluation times for indicators")
            return True
        df[INDICATOR_PAYLOAD_EVALUATION_TIMES_COL] = (
            df[INDICATOR_PAYLOAD_EVALUATION_TIMES_COL].astype(int) / 1000
        ).astype(int)
        if sketch_type_in_hash:
            df[INDICATOR_PAYLOAD_SKETCH_TYPE] = df.apply(
                lambda row: SKETCH_TYPE_NONE
                if self.get_sketch_type_from_calculation(
                    row[INDICATOR_PAYLOAD_CALCULATION_COL]
                )
                == SKETCH_TYPE_NONE
                else row[INDICATOR_PAYLOAD_CALCULATION_COL],
                axis=1,
            )
            df[INDICATOR_PAYLOAD_COMPUTE_HASH_COL] = df.apply(
                lambda row: lariat_sql_utils.get_hashed_fields(
                    group_fields=row[INDICATOR_PAYLOAD_GROUP_FIELDS_COL],
                    filter_str=row[INDICATOR_PAYLOAD_FILTERS_COL],
                    evaluation_interval=row[INDICATOR_PAYLOAD_EVALUATION_INTERVAL_COL],
                    lookback_window=row[INDICATOR_PAYLOAD_LOOKBACK_WINDOW_LENGTH_COL],
                    computed_dataset_id=row[INDICATOR_PAYLOAD_COMPUTED_DATASET_ID_COL],
                    sketch_type=row[INDICATOR_PAYLOAD_SKETCH_TYPE],
                ),
                axis=1,
            )
        else:
            df[INDICATOR_PAYLOAD_COMPUTE_HASH_COL] = df.apply(
                lambda row: lariat_sql_utils.get_hashed_fields(
                    group_fields=row[INDICATOR_PAYLOAD_GROUP_FIELDS_COL],
                    filter_str=row[INDICATOR_PAYLOAD_FILTERS_COL],
                    evaluation_interval=row[INDICATOR_PAYLOAD_EVALUATION_INTERVAL_COL],
                    lookback_window=row[INDICATOR_PAYLOAD_LOOKBACK_WINDOW_LENGTH_COL],
                    computed_dataset_id=row[INDICATOR_PAYLOAD_COMPUTED_DATASET_ID_COL],
                ),
                axis=1,
            )
        grouped_df = df.groupby(
            [
                INDICATOR_PAYLOAD_COMPUTE_HASH_COL,
                INDICATOR_PAYLOAD_EVALUATION_TIMES_COL,
                ORG_ID,
            ],
            dropna=False,
        )
        for (compute_hash, evaluation_time, org_id), df_group in grouped_df:
            calculation_indicator_id_pairs = list(
                zip(
                    df_group[INDICATOR_PAYLOAD_CALCULATION_COL],
                    df_group[INDICATOR_PAYLOAD_INDICATOR_ID_COL],
                )
            )
            first_row = df_group.iloc[0]
            group_fields = str(first_row[INDICATOR_PAYLOAD_GROUP_FIELDS_COL])
            computed_dataset_query = first_row[
                INDICATOR_PAYLOAD_COMPUTED_DATASET_QUERY_COL
            ].removesuffix(";")
            timestamp_column = first_row[INDICATOR_PAYLOAD_TIMESTAMP_COLUMN_COL]
            lookback_window_length = first_row[
                INDICATOR_PAYLOAD_LOOKBACK_WINDOW_LENGTH_COL
            ]
            filter_str = first_row[INDICATOR_PAYLOAD_FILTERS_COL]
            if group_fields:
                group_fields = group_fields.split(",")
            self.create_tags_post_indicator_dispatch(df_group)
            query = self.query_builder.build(
                computed_dataset_query=computed_dataset_query,
                calculation_indicator_id_pairs=calculation_indicator_id_pairs,
                group_fields=group_fields,
                timestamp_field=timestamp_column,
                evaluation_time=evaluation_time,
                lookback_time=lookback_window_length,
                filter_str=filter_str,
            )
            indicator_query_output_key = f"{INDICATOR_QUERY_OUTPUT_KEY_PREFIX}/org_id={org_id}/source_id={source_id}"
            ingestion_time = datetime.datetime.utcnow()
            query_output_path = (
                f"{indicator_query_output_key.strip('/')}/{compute_hash}/"
                f"year={ingestion_time.year}/month={str(ingestion_time.month).zfill(2)}/"
                f"day={str(ingestion_time.day).zfill(2)}/hour={str(ingestion_time.hour).zfill(2)}/"
                f"minute={str(ingestion_time.minute).zfill(2)}/"
            )
            logging.debug(f"Running Query: {query}")
            logging.debug(f"Sending Data to: {query_output_path}")

            output_df, indicator_statuses = self.query_builder.run(
                query=query,
                output_path=query_output_path,
            )
            if indicator_statuses:
                self.send_payload_to_agent(
                    payload=indicator_statuses, endpoint=LARIAT_INDICATOR_STATUS_URL
                )
            if expect_results:
                source_file_path = Path(
                    query_output_path, f"result_{int(time.time())}"
                ).as_posix()
                self.write_data(
                    result_df=output_df,
                    source_top_level=None,
                    source_file_path=source_file_path,
                )
            time.sleep(AGENT_INTER_QUERY_GAP)

    def write_data(
        self,
        result_df: pd.DataFrame = None,
        source_top_level: str = None,
        source_file_path: str = None,
        indicator_statuses: List = None,
        updated_tags: Dict = None,
    ):
        """
        This function is responsible for retrieving results and passing them over to the defined sink's write function
        :param result_df: dataframe representing results for a given query run (Note: this may include
        multiple indicator results due to optimizations)
        :param source_top_level: If writing out to a location, this holds information of the s3 bucket, azure blob etc.
        :param source_file_path: This represents the object key or rest of the file path associated with the results
        :param indicator_statuses: If there are relevant updates to indicator status, this gets sent to the Lariat
        platform
        :param updated_tags: Additional tags to include based on metadata received from query runs
        :return: No return value
        """
        result_df.columns = [
            col.lower()
            if (
                col in RESULT_DF_RESERVED_FIELDS
                or col.startswith(RESULTS_DF_INDICATOR_COL_PREFIX)
            )
            else col
            for col in result_df.columns
        ]
        indicator_ids = [
            int(col.removeprefix(RESULTS_DF_INDICATOR_COL_PREFIX))
            for col in result_df.columns
            if col.startswith(RESULTS_DF_INDICATOR_COL_PREFIX)
        ]
        tags_dict = self.create_tags_post_query_completion(
            indicator_id_list=indicator_ids, updated_tags=updated_tags
        )
        self.sink.write(
            result_df=result_df,
            source_top_level=source_top_level,
            file_path=source_file_path,
            tags_dict=tags_dict,
        )

        if indicator_statuses:
            self.send_payload_to_agent(
                payload=indicator_statuses, endpoint=LARIAT_INDICATOR_STATUS_URL
            )

    @staticmethod
    def get_sketch_type_from_calculation(calculation) -> str:
        """
        Parse a calculation and return whether it is a SKETCH_DISTINCT, SKETCH_DECILE or SKETCH_TYPE_NONE based
        on whether the calculation is a qunatile, distinct or neither
        :param calculation: sql predicate representing the calculation of an indicator value (e.g. COUNT(DISTINCT xyz))
        :return: One of COUNT_DISTINCT, DECILE, or None
        """
        statement = sqlparse.parse(calculation)[0]
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
                return SKETCH_TYPE_DISTINCT
            if is_match_decile:
                return SKETCH_TYPE_DECILE
        return SKETCH_TYPE_NONE

    def create_tags_post_indicator_dispatch(
        self, indicator_df_group: pd.DataFrame, additional_metadata=None
    ) -> bool:
        """
        Prepares tags relevant to indicators that can be consumed by sinks.
        This does so on a per indicator basis, and takes in the input of the indicator payload
        on a per compute hash basis.
        Note: You can override this function without knowing the details of the group key and can safely assume
        this dataframe has all of the fields available in the Indicator Payload
        Side Effect: This writes out the tags to the underlying cloud store / local store that the agent is operating
        in. This is then read back in by the create_tags_post_query_completion function on write and passed onto the
        sink.
        :param indicator_df_group: Group of indicators being computed
        :param additional_metadata
        :return: boolean indicating success of tag creation
        """
        if self.sink.supports_tags:
            try:
                tags_from_single_values = dict(
                    zip(
                        indicator_df_group[INDICATOR_PAYLOAD_INDICATOR_ID_COL],
                        iter(
                            [
                                list(entry)
                                for entry in zip(
                                    "name:"
                                    + indicator_df_group[
                                        INDICATOR_PAYLOAD_INDICATOR_NAME_COL
                                    ].astype(str),
                                    "delay_seconds:"
                                    + indicator_df_group[
                                        INDICATOR_PAYLOAD_LAG_COL
                                    ].astype(str),
                                    "timestamp_col:"
                                    + indicator_df_group[
                                        INDICATOR_PAYLOAD_TIMESTAMP_COLUMN_COL
                                    ],
                                    "data_source:"
                                    + indicator_df_group[
                                        INDICATOR_PAYLOAD_COMPUTED_DATASET_SOURCE_COL
                                    ].astype(str),
                                    "lookback_seconds:"
                                    + indicator_df_group[
                                        INDICATOR_PAYLOAD_LOOKBACK_WINDOW_LENGTH_COL
                                    ].astype(str),
                                )
                            ],
                        ),
                    )
                )
                tags_from_list_fields = dict(
                    zip(
                        indicator_df_group[INDICATOR_PAYLOAD_INDICATOR_ID_COL],
                        iter(
                            [
                                [
                                    f"dataset:{raw_dataset}"
                                    for raw_dataset in raw_dataset_list
                                ]
                                for raw_dataset_list in indicator_df_group[
                                    INDICATOR_PAYLOAD_INDICATOR_RAW_DATASETS_COL
                                ]
                            ]
                        ),
                    )
                )
                tag_dict = tags_from_single_values
                for key, value in tags_from_list_fields.items():
                    if key in tag_dict:
                        tag_dict[key].extend(value)
                    else:
                        tag_dict[key] = value
                self.persist_tags_dict(tag_dict)
                return True
            except Exception as e:
                logging.warning(f"Failed to build tags from indicator payload {str(e)}")
                return False

    def create_tags_post_query_completion(
        self,
        indicator_id_list: List,
        updated_tags: Dict = None,
    ) -> Dict:
        """
        Allow for the addition of post query tags. E.g. one might want to add in query completion time, bytes read etc.
        as a tag.
        :param indicator_id_list - list of indicator_ids to apply the tag to
        :param updated_tags - a tag dictionary of id to tags
        :return: a dictionary representing the final tag dictionary to be passed to the sink. This is of the format
        {$indicator_id: ["$tag_key1:$tag_value1", "$tag_key2:$tag_value2"]}
        """
        if self.sink.supports_tags:
            tag_dict = self.retrieve_tags_dict(indicator_id_list)
            if updated_tags:
                for key, value in updated_tags.items():
                    if key in tag_dict:
                        tag_dict[key].extend(value)
                    else:
                        tag_dict[key] = value
            return tag_dict
        return {}

    def get_tag_path_elements(self, indicator_id):
        """
        Return the tag path based on the cloud being used
        """
        output_key = f"{TAG_FILESYSTEM_PREFIX}/{indicator_id}.json"
        if self._cloud == CLOUD_TYPE_AWS:
            s3_agent_path = self._cloud_agent_config_path
            s3path_obj = S3Path(f"/{s3_agent_path}")
            path_elements = (s3path_obj.bucket, output_key)
        elif self._cloud == CLOUD_TYPE_AZURE:
            path_elements = (
                output_key,
                AZURE_STORAGE_CONFIG_CONTAINER,
                AZURE_STORAGE_CONFIG_CONNECTION_STRING,
            )
        elif self._cloud == CLOUD_TYPE_NONE:
            path_elements = f"{os.path.dirname(self._cloud_agent_config_path)}{os.path.pathsep}{output_key})"
        else:
            return ValueError(f"Unsupported Cloud for tags: {self._cloud}")
        return path_elements

    def persist_tags_dict(self, tag_dict: Dict):
        """
        Write tags out to the cloud of choice
        """
        for indicator_id, tag_data in tag_dict.items():
            path_elements = self.get_tag_path_elements(indicator_id)
            json_buffer = StringIO()
            json.dump({indicator_id: tag_dict[indicator_id]}, json_buffer)
            write_stringio_to_cloud(json_buffer, path_elements, self._cloud)

    def retrieve_tags_dict(self, indicator_list):
        """
        Retrieve the tags from the cloud of choice for a given indicator
        """
        tags_dict = {}
        for indicator_id in indicator_list:
            path_elements = self.get_tag_path_elements(indicator_id)
            indicator_dict = json.loads(
                read_stringio_from_cloud(path_elements, self._cloud).getvalue()
            )
            if str(indicator_id) in indicator_dict:
                tags_dict[indicator_id] = indicator_dict[str(indicator_id)]
        return tags_dict
