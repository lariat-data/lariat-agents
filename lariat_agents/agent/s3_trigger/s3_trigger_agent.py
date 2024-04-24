from lariat_agents.base.batch_base import BatchBaseAgent
from boto3_type_annotations import s3
import json
from genson import SchemaBuilder
from lariat_python_common.schema.utils import get_clean_schema
from lariat_python_common.string.utils import match_lariat_file_partition_pattern
from lariat_agents.constants import LARIAT_EVENT_NAME, LARIAT_PROCESS_SCHEMA_URL
from lariat_agents.agent.s3_trigger.s3_trigger_query_builder import (
    S3TriggerQueryBuilder,
)
from datetime import datetime
import croniter


class S3TriggerAgent(BatchBaseAgent):
    def __init__(
        self,
        agent_type: str,
        cloud: str,
        s3_handler: s3.Client,
        api_key: str = None,
        application_key: str = None,
    ):
        self.s3_handler = s3_handler
        super().__init__(
            agent_type=agent_type,
            cloud=cloud,
            api_key=api_key,
            application_key=application_key,
            query_builder=S3TriggerQueryBuilder(
                query_builder_type=agent_type,
                name_data_map=None,
                raw_dataset_names=None,
            ),
        )

    @staticmethod
    def get_next_evaluation_time(evaluation_interval):
        try:
            schedule_obj = croniter.croniter(evaluation_interval)
        except Exception as err:
            # Handle the error
            print(f"Error parsing cron expression: {err}")
            schedule_obj = None

        return schedule_obj

    @staticmethod
    def get_evaluation_times(
        evaluation_interval, last_evaluation_time, max_time, indicator_process_delay
    ):
        schedule = S3TriggerAgent.get_next_evaluation_time(evaluation_interval)
        next_evaluation_time = last_evaluation_time
        adjusted_max_time = max_time - indicator_process_delay
        evaluation_times = []
        while next_evaluation_time <= adjusted_max_time:
            next_evaluation_time = schedule.get_next(
                datetime, start_time=next_evaluation_time
            )
            evaluation_times.append(next_evaluation_time.strftime("%Y-%m-%d %H:%M:%S"))
        return evaluation_times

    def schema_retrieval(self, event_dict):
        output_schema_map = {}
        name_data_map = {}
        source_id = None
        agent_config = self.get_yaml_config()
        if "Records" in event_dict:
            for record in event_dict["Records"]:
                if "s3" in record:
                    s3_record = record["s3"]
                    bucket_name = s3_record["bucket"]["name"]
                    object_key = s3_record["object"]["key"]
                    if bucket_name in agent_config["buckets"]:
                        bucket_configs = agent_config["buckets"][bucket_name]
                        for bucket_config in bucket_configs:
                            if object_key.startswith(
                                bucket_config.get("prefix")
                            ):  # Matches prefix
                                suffix_key = object_key[
                                    len(bucket_config.get("prefix")) :
                                ]
                                additional_fields_in_schema = (
                                    match_lariat_file_partition_pattern(
                                        suffix_key, bucket_config.get("suffix_template")
                                    )
                                )
                                if additional_fields_in_schema:
                                    # Retrieve data
                                    response = self.s3_handler.get_object(
                                        Bucket=bucket_name, Key=object_key
                                    )
                                    file_type = bucket_config.get("file_type")
                                    if file_type == "json":
                                        file_content = (
                                            response["Body"].read().decode("utf-8")
                                        )
                                        if bucket_config.get("lines"):
                                            jsonl_lines = file_content.splitlines()
                                            final_merged_data = {}
                                            for json_line in jsonl_lines:
                                                object_data = json.loads(json_line)
                                                final_merged_data.update(object_data)
                                        else:
                                            final_merged_data = json.loads(file_content)

                                        source_id = additional_fields_in_schema.get(
                                            "source_id"
                                        )
                                        final_merged_data.update(
                                            additional_fields_in_schema
                                        )
                                        builder = SchemaBuilder()
                                        builder.add_object(final_merged_data)
                                        clean_schema = get_clean_schema(builder)
                                        output_schema_map[
                                            bucket_config.get("name")
                                        ] = clean_schema
                                        name_data_map[
                                            bucket_config.get("name")
                                        ] = final_merged_data
        output_schema_list = []
        for name, clean_schema in output_schema_map.items():
            output_schema = {
                "dataset_schema": clean_schema,
                "raw_dataset_name": name,
                "raw_dataset_data_source": "s3_trigger",
                "raw_dataset_source_id": source_id,
                "raw_dataset_event_name": LARIAT_EVENT_NAME,
            }
            output_schema_list.append(output_schema)
        self.query_builder.name_data_map = name_data_map
        indicators = self.get_lariat_indicator_json_from_streaming(
            endpoint=LARIAT_PROCESS_SCHEMA_URL,
            payload={"raw_datasets": output_schema_list},
        )
        raw_dataset_names = []
        for index, row in indicators.iterrows():
            if "raw_dataset_names" in row:
                raw_dataset_names.extend(row["raw_dataset_names"])
        self.query_builder.raw_dataset_names = list(set(raw_dataset_names))
        return indicators

    def map_action_to_function(self, action, event_dict=None):
        """
        Supported actions:
        - raw_schema: request the raw_schema based on the s3 pattern specified in the config yaml
        :param action: One of the supported actions for the agent to run
        :param event_dict: Any additional event specific data
        :return:
        """
        indicators = self.schema_retrieval(event_dict)
        self.execute_indicators(
            indicators=indicators,
            expect_results=True,
            name_data_map=self.query_builder.name_data_map,
            raw_dataset_names=self.query_builder.raw_dataset_names,
        )
