from lariat_agents.agent.event_payload.event_payload_types import (
    EventPayload,
    EventType,
    PayloadSource,
)
from typing import Dict, List
from urllib.parse import unquote_plus
import json
from lariat_python_common.string.utils import match_lariat_file_partition_pattern

DEFAULT_PARTITION_SEPARATOR = "="


def process_sns_s3_event(
    event_obj: Dict, payload_source: PayloadSource
) -> List[EventPayload]:
    event_payload_list = []
    for record in event_obj["Records"]:
        trigger_events = json.loads(record["Sns"]["Message"])
        for trigger_event in trigger_events["Records"]:
            input_bucket_name = trigger_event["s3"]["bucket"]["name"]
            input_object_name = unquote_plus(
                trigger_event["s3"]["object"]["key"], encoding="utf-8"
            )
            event_payload_list.append(
                EventPayload(
                    event_source=EventType.SNS_S3_TRIGGER.value,
                    bucket=input_bucket_name,
                    object_key=input_object_name,
                    payload_source=payload_source,
                    raw_event=trigger_event,
                )
            )
    return event_payload_list


def process_s3_trigger_event(
    event_obj: Dict, payload_source: PayloadSource
) -> List[EventPayload]:
    event_payload_list = []
    for trigger_event in event_obj["Records"]:
        input_bucket_name = trigger_event["s3"]["bucket"]["name"]
        input_object_name = unquote_plus(
            trigger_event["s3"]["object"]["key"], encoding="utf-8"
        )
        event_payload_list.append(
            EventPayload(
                event_source=EventType.S3_TRIGGER.value,
                bucket=input_bucket_name,
                object_key=input_object_name,
                payload_source=payload_source,
                raw_event=trigger_event,
            )
        )
    return event_payload_list


def process_event_payload(
    event_obj: Dict, payload_source: PayloadSource
) -> List[EventPayload]:
    if "requestPayload" in event_obj:
        # Handles the case when using a lambda definition wraps the event in requestPayload
        event_obj = event_obj["requestPayload"]

    if "Records" in event_obj and "s3" in event_obj["Records"][0]:
        event_type = EventType.S3_TRIGGER
    elif "Records" in event_obj and "Sns" in event_obj["Records"][0]:
        event_type = EventType.SNS_S3_TRIGGER
    else:
        raise ValueError(f"Unsupported event object passed i: {event_obj}")
    if event_type == EventType.SNS_S3_TRIGGER:
        return process_sns_s3_event(event_obj, payload_source)
    elif event_type == EventType.S3_TRIGGER:
        return process_s3_trigger_event(event_obj, payload_source)


def collect_payload_from_s3(agent_config, bucket_name, object_key, s3_handler):
    if bucket_name in agent_config["buckets"]:
        bucket_configs = agent_config["buckets"][bucket_name]
        for bucket_config in bucket_configs:
            if object_key.startswith(bucket_config.get("prefix")):  # Matches prefix
                suffix_key = (
                    object_key[len(bucket_config.get("prefix")) :]
                    .removeprefix("/")
                    .removesuffix("/")
                )
                partition_separator = bucket_config.get(
                    "partition_separator", DEFAULT_PARTITION_SEPARATOR
                )
                partition_fields_in_data = match_lariat_file_partition_pattern(
                    suffix_key,
                    bucket_config.get("suffix_template"),
                    partition_separator,
                )
                if partition_fields_in_data:
                    # Retrieve data
                    response = s3_handler.get_object(Bucket=bucket_name, Key=object_key)
                    file_content = response["Body"].read()
                    file_type = bucket_config.get("file_type")
                    config_name = bucket_config.get("name")
                    return (
                        file_type,
                        file_content,
                        config_name,
                        partition_fields_in_data,
                    )
    return None, None, None, None
