import urllib.error
from abc import ABC, abstractmethod
from typing import Dict
from lariat_agents.constants import (
    CLOUD_TYPE_AZURE,
    CLOUD_TYPE_AWS,
    CLOUD_TYPE_NONE,
    CLOUD_TYPE_GCP,
    CLOUD_AGENT_CONFIG_PATH,
    AZURE_STORAGE_CONFIG_CONTAINER,
    AZURE_STORAGE_CONFIG_CONNECTION_STRING,
    LARIAT_BASE_URL,
    SINK_TYPE,
    LARIAT_SINK_TYPE,
    DATADOG_SINK_TYPE,
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
from urllib.parse import urlencode
from urllib.error import HTTPError
import logging
import pandas as pd
import lariat_python_common.sql.utils as lariat_sql_utils
from lariat_agents.base.streaming_base.streaming_base_query_builder import (
    StreamingBaseQueryBuilder,
)
from lariat_agents.sink.lariat_sink import LariatSink
from lariat_agents.sink.datadog_sink import DatadogSink
import sqlparse
from google.cloud import storage


class StreamingBaseAgent(ABC):
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
    changed in the yaml config, that is automatically customized by the batch_base agent).
    N.B.: When new sinks are added, the constructor of this class needs to be aware in order to make sure
    they send data to the correctly configured sink.
    """

    def __init__(
        self,
        agent_type: str,
        cloud: str,
        query_builder: StreamingBaseQueryBuilder,
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
        elif self._cloud == CLOUD_TYPE_GCP:
            self._cloud_agent_config_path = CLOUD_AGENT_CONFIG_PATH
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
    def schema_retrieval(self, event_dict=None):
        """
        Read the yaml config, retrieve schemas from the agent store.
        Register the schema with the Lariat endpoint
        :param event_dict: contains any relevant information about a triggering process
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
        elif self._cloud == CLOUD_TYPE_GCP:
            gcs_handler = storage.Client()
            bucket, object_path = self._cloud_agent_config_path.split("/", 1)
            gcs_bucket = gcs_handler.get_bucket(bucket)
            blob = gcs_bucket.get_blob(object_path)
            agent_config = yaml.load(blob.download_as_string())
        else:
            raise ValueError(f"Unsupported cloud environment: {self._cloud}")
        return agent_config

    def validate_api_keys(self):
        base_url = LARIAT_BASE_URL.removesuffix("/api")
        req = request.Request(f"{base_url}/authenticated_ping")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Lariat-Api-Key", self._api_key)
        req.add_header("X-Lariat-Application-Key", self._application_key)
        try:
            with request.urlopen(req) as url:
                url.read().decode()
        except urllib.error.HTTPError as e:
            if e.status == 401:
                return False
        return True

    def send_payload_to_agent(self, payload, endpoint, params=None) -> bool:
        """
        Send the JSON payloads back to the approriate Lariat endpoint with the API Key and Application Key
        correctly defined
        :param payload: JSON representing the data that needs to be sent to the web service
        :param endpoint: fully qualifed url to which the data is sent
        :param params: Query params dict for http request (doesn't have to be encoded)
        :return: True on success and False on failure
        """
        data = json.dumps(payload)
        data = data.encode("utf-8")
        if params is None:
            req = request.Request(endpoint, data=data)
        else:
            encoded_params = urlencode(params)
            req = request.Request(endpoint + "?" + encoded_params, data=data)
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Lariat-Api-Key", self._api_key)
        req.add_header("X-Lariat-Application-Key", self._application_key)
        try:
            request.urlopen(req)
        except HTTPError as e:
            logging.error(f"Failed to reach ingest service: {endpoint} {e}")
            return False
        return True

    def get_lariat_indicator_json_from_streaming(self, endpoint, payload):
        """
        Sends a request to the Lariat Server to receive a list of indicators and evaluation times
        to collect health metrics for given the schema streaming payload.

        The schema payload is what makes this different

        :return: Pandas dataframe with list of indicators that each have a list of evaluation times and other
        relevant information such as name, group_fields
        """
        data = json.dumps(payload)
        data = data.encode("utf-8")
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Lariat-Api-Key": self._api_key,
            "X-Lariat-Application-Key": self._application_key,
        }
        req = request.Request(url=endpoint, data=data, headers=headers)
        with request.urlopen(req) as url:
            data = pd.read_json(url.read().decode()).fillna("")
        return data

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

    def write_data(
        self,
        result_df: pd.DataFrame = None,
        source_top_level: str = None,
        source_file_path: str = None,
    ):
        """
        This function is responsible for retrieving results and passing them over to the defined sink's write function
        :param result_df: dataframe representing results for a given query run (Note: this may include
        multiple indicator results due to optimizations)
        :param source_top_level: If writing out to a location, this holds information of the s3 bucket, azure blob etc.
        :param source_file_path: This represents the object key or rest of the file path associated with the results
        :return: No return value
        """
        if result_df is not None:
            result_df.columns = [
                col.lower()
                if (
                    col in RESULT_DF_RESERVED_FIELDS
                    or col.startswith(RESULTS_DF_INDICATOR_COL_PREFIX)
                )
                else col
                for col in result_df.columns
            ]

        self.sink.write(
            result_df=result_df,
            source_top_level=source_top_level,
            file_path=source_file_path,
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
