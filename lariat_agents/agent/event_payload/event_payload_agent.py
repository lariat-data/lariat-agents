from lariat_agents.base.streaming_base.streaming_base_agent import StreamingBaseAgent
from boto3_type_annotations import s3

from lariat_agents.constants import (
    EVENT_PAYLOAD_OUTPUT_KEY_PREFIX,
    LARIAT_PAYLOAD_SOURCE,
    LARIAT_BASE_URL,
    PERSIST_UNIQUE_DIMENSION_VALUES,
)
from lariat_agents.agent.event_payload.event_payload_query_builder import (
    EventPayloadQueryBuilder,
)
from datetime import datetime
from lariat_agents.agent.event_payload.event_payload_types import (
    SupportedPayloadFormat,
)
from lariat_agents.agent.event_payload.event_payload_utils import (
    process_event_payload,
    EventPayload,
    PayloadSource,
    collect_payload_from_s3,
    collect_payload_from_gcs,
)
from typing import List
import logging
from pathlib import Path
import time
import hashlib


class EventPayloadAgent(StreamingBaseAgent):
    def __init__(
        self,
        agent_type: str,
        cloud: str,
        s3_handler: s3.Client = None,
        gcs_handler=None,
        api_key: str = None,
        application_key: str = None,
    ):
        self.s3_handler = s3_handler
        self.gcs_handler = gcs_handler

        super().__init__(
            agent_type=agent_type,
            cloud=cloud,
            api_key=api_key,
            application_key=application_key,
            query_builder=EventPayloadQueryBuilder(
                query_builder_type=agent_type,
                name_data_map=None,
                raw_dataset_names=None,
            ),
        )

    def schema_retrieval(self, event_info: List[EventPayload] = None):
        output_schema_map = {}
        name_data_map = {}
        agent_config = self.yaml_config
        for record in event_info:
            if record.payload_source == PayloadSource.S3:
                bucket_name = record.bucket

                object_key = record.object_key
                (
                    file_type,
                    fsspec_name,
                    compression,
                    clean_schema,
                    config_name,
                    partition_fields_in_data,
                    content_length,
                ) = collect_payload_from_s3(
                    agent_config, bucket_name, object_key, self.s3_handler
                )
            elif record.payload_source == PayloadSource.GCS:
                bucket_name = record.bucket

                object_key = record.object_key
                (
                    file_type,
                    fsspec_name,
                    compression,
                    clean_schema,
                    config_name,
                    partition_fields_in_data,
                    content_length,
                ) = collect_payload_from_gcs(
                    agent_config, bucket_name, object_key, self.gcs_handler
                )
            else:
                config_name = None
                partition_fields_in_data = None
                bucket_name = None
                object_key = None
                clean_schema = None
                fsspec_name = None
                content_length = 0

            if clean_schema:
                output_schema_map[config_name] = clean_schema
                name_data_map[config_name] = (
                    fsspec_name,
                    partition_fields_in_data,
                    clean_schema,
                    bucket_name,
                    object_key,
                    record.raw_event,
                    content_length,
                )
            else:
                logging.info(
                    f"Object Not Associated with Config found: {bucket_name} {object_key}"
                )
        return name_data_map

    def execute_stream_metrics(self, name_data_map, parent_event):
        events = []
        for dataset_name in name_data_map:
            for file_family in self.yaml_config["buckets"].values():
                for prefix_item in file_family:
                    if prefix_item.get("name") == dataset_name:
                        string_columns = []
                        numeric_columns = []
                        timestamp_mappings = {}
                        dimensions = prefix_item.get("dimensions", [])
                        if (
                            "columns" in prefix_item
                            and "string" in prefix_item["columns"]
                        ):
                            string_columns = prefix_item.get("columns", {}).get(
                                "string", []
                            )
                            if string_columns is None:
                                string_columns = []
                        if (
                            "columns" in prefix_item
                            and "number" in prefix_item["columns"]
                        ):
                            numeric_columns = prefix_item.get("columns", {}).get(
                                "number", []
                            )
                            if numeric_columns is None:
                                numeric_columns = []
                        if "timestamp" in prefix_item:
                            timestamp_mappings = prefix_item.get("timestamp", {})

                        (
                            fsspec_name,
                            partition_fields_in_data,
                            clean_schema,
                            bucket_name,
                            object_key,
                            raw_event,
                            content_length,
                        ) = name_data_map[dataset_name]
                        source_id = self.yaml_config["source_id"]
                        (
                            output_df,
                            execution_time,
                            primary_time_column,
                            filtered_dimensions,
                        ) = self.query_builder.run(
                            fsspec_name,
                            partition_fields_in_data,
                            clean_schema,
                            SupportedPayloadFormat(prefix_item.get("file_type")),
                            string_columns,
                            numeric_columns,
                            timestamp_mappings,
                            dimensions,
                            source_id,
                            dataset_name,
                            (bucket_name, object_key),
                            content_length,
                        )
                        if output_df is not None:
                            min_primary_time = (
                                output_df[primary_time_column].min().item()
                                if primary_time_column
                                else None
                            )
                            max_primary_time = (
                                output_df[primary_time_column].max().item()
                                if primary_time_column
                                else None
                            )
                            event_dict = {
                                "input_event": raw_event,
                                "parent_event": parent_event,
                                "schema": clean_schema,
                                "lariat_agent_execution_time": execution_time,
                                "min_primary_time": min_primary_time,
                                "max_primary_time": max_primary_time,
                                "primary_time_column": primary_time_column,
                                "lariat_dataset_name": dataset_name,
                            }

                            if filtered_dimensions and PERSIST_UNIQUE_DIMENSION_VALUES:
                                unique_dimension_values = {
                                    dim: output_df[f"dim|{dim}"]
                                    .drop_duplicates()
                                    .tolist()
                                    for dim in filtered_dimensions
                                }
                                event_dict["dimensions"] = unique_dimension_values
                            else:
                                event_dict["dimensions"] = {}
                            events.append(event_dict)
                            indicator_query_output_key = (
                                f"{EVENT_PAYLOAD_OUTPUT_KEY_PREFIX}/"
                                f"api_key={self._api_key}/source_id={source_id}/dataset={dataset_name}"
                            )
                            ingestion_time = datetime.utcnow()
                            query_output_path = (
                                f"{indicator_query_output_key.strip('/')}/"
                                f"year={ingestion_time.year}/month={str(ingestion_time.month).zfill(2)}/"
                                f"day={str(ingestion_time.day).zfill(2)}/hour={str(ingestion_time.hour).zfill(2)}/"
                                f"minute={str(ingestion_time.minute).zfill(2)}/"
                            )
                            hash_object = hashlib.sha1(object_key.encode())
                            source_file_path = Path(
                                query_output_path,
                                f"result_{str(hash_object.hexdigest())}_{int(time.time())}.csv",
                            ).as_posix()
                            self.write_data(
                                output_df, source_file_path=source_file_path
                            )
                        else:
                            event_dict = {
                                "input_event": raw_event,
                                "parent_event": parent_event,
                                "schema": clean_schema,
                                "lariat_agent_execution_time": execution_time,
                                "primary_time_column": primary_time_column,
                                "lariat_dataset_name": dataset_name,
                            }
                            events.append(event_dict)
                            logging.warning(
                                f"Data could not be written for {bucket_name} {object_key} "
                            )

        return events

    def map_action_to_function(self, action, event_dict=None):
        """
        Supported actions:
        - raw_schema: request the raw_schema based on the s3 pattern specified in the config yaml
        :param action: One of the supported actions for the agent to run
        :param event_dict: Any additional event specific data
        :return:
        """
        are_keys_valid = self.validate_api_keys()
        if are_keys_valid:
            event_payload_list = process_event_payload(
                event_dict, PayloadSource(LARIAT_PAYLOAD_SOURCE), self._cloud
            )
            name_data_map = self.schema_retrieval(event_payload_list)
            events_list = self.execute_stream_metrics(name_data_map, event_dict)
            if events_list:
                payload = {"events": events_list}
                params = {"sourceId": self.yaml_config["source_id"]}
                self.send_payload_to_agent(
                    endpoint=f"{LARIAT_BASE_URL.removesuffix('/')}/ingest_s3_events",
                    payload=payload,
                    params=params,
                )
        else:
            logging.error("Failed to authenticate credentials")
            raise PermissionError("Couldn't authenticate Api & Application keypair")
