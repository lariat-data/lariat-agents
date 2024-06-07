import pandas as pd
from typing import Optional
from lariat_agents.agent.event_payload.event_payload_types import (
    EventPayload,
    EventType,
    PayloadSource,
    CompressionType,
    SupportedPayloadFormat,
)
from typing import Dict, List
from urllib.parse import unquote_plus
import json
from lariat_python_common.string.utils import match_lariat_file_partition_pattern
import magic
from lariat_agents.constants import EVENT_PAYLOAD_MAX_CONTENT_LENGTH_BYTES
from lariat_python_common.schema.utils import (
    get_schema_from_json,
    get_schema_from_df,
)
from lariat_python_common.pandas.utils import get_df_iterator_from_avro

import pyarrow.parquet as pq
import fsspec
from lariat_agents.constants import (
    CLOUD_TYPE_AWS,
    CLOUD_TYPE_NONE,
    CLOUD_TYPE_GCP,
)
import logging


DEFAULT_PARTITION_SEPARATOR = "="
META_PREFIX = "partition."
OBJECT_PREFIX = "object."


def is_content_too_large(content_length) -> bool:
    return content_length >= EVENT_PAYLOAD_MAX_CONTENT_LENGTH_BYTES


def get_schema_for_data(
    file_location, file_type, partition_fields_in_data, chunksize: Optional[int] = 10000
):
    clean_schema = None
    file_type = SupportedPayloadFormat(file_type)
    if file_type == SupportedPayloadFormat.JSONL:
        chunks = pd.read_json(file_location, lines=True, chunksize=chunksize)
        if chunksize:
            df = next(chunks, None)
        else:
            df = chunks
        if df is not None:
            final_merged_data = df.to_json(orient="records")
            clean_schema = get_schema_from_json(
                final_merged_data,
                partition_fields_in_data,
                OBJECT_PREFIX,
                META_PREFIX,
            )

    elif file_type == SupportedPayloadFormat.JSON:
        chunks = pd.read_json(file_location, lines=False, chunksize=chunksize)
        if chunksize:
            df = next(chunks, None)
        else:
            df = chunks
        if df is not None:
            final_merged_data = df.to_json(orient="records")
            clean_schema = get_schema_from_json(
                final_merged_data,
                partition_fields_in_data,
                OBJECT_PREFIX,
                META_PREFIX,
            )
    elif file_type == SupportedPayloadFormat.CSV:
        chunks = pd.read_csv(file_location, chunksize=chunksize, on_bad_lines="warn")
        if chunksize:
            df = next(chunks, None)
        else:
            df = chunks
        if df is not None:
            clean_schema = get_schema_from_df(
                df,
                partition_fields_in_data,
                OBJECT_PREFIX,
                META_PREFIX,
            )
    elif file_type == SupportedPayloadFormat.PARQUET:
        if chunksize is None:
            df = pd.read_parquet(file_location)
        else:
            with fsspec.open(file_location) as f:
                parquet_file = pq.ParquetFile(f)
                df = next(
                    parquet_file.iter_batches(batch_size=chunksize), None
                ).to_pandas()
        clean_schema = get_schema_from_df(
            df,
            partition_fields_in_data,
            OBJECT_PREFIX,
            META_PREFIX,
        )
    elif file_type == SupportedPayloadFormat.AVRO:
        df = next(get_df_iterator_from_avro(file_location, chunksize), None)
        clean_schema = get_schema_from_df(
            df,
            partition_fields_in_data,
            OBJECT_PREFIX,
            META_PREFIX,
        )
    return clean_schema


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


def process_gcs_object_event(
    event_obj: Dict, payload_source: PayloadSource
) -> List[EventPayload]:
    input_bucket_name = event_obj.get("data").get("bucket")
    object_key = event_obj.get("data").get("name")
    if not input_bucket_name or not object_key:
        logging.error("Missing bucket and object")
    event_payload_list = [
        EventPayload(
            event_source=EventType.GCS_TRIGGER.value,
            bucket=input_bucket_name,
            object_key=object_key,
            payload_source=payload_source,
            raw_event=event_obj,
        )
    ]
    return event_payload_list


def process_event_payload(
    event_obj: Dict, payload_source: PayloadSource, cloud_mode: str
) -> List[EventPayload]:
    if cloud_mode == CLOUD_TYPE_NONE:
        cloud_mode = CLOUD_TYPE_AWS
    if cloud_mode == CLOUD_TYPE_AWS:
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
    elif cloud_mode == CLOUD_TYPE_GCP:
        return process_gcs_object_event(event_obj, payload_source)


def get_compression_from_magic_type(compression_magic_type):
    if "gzip" in compression_magic_type:
        return CompressionType.GZIP
    elif "gzip2" in compression_magic_type:
        return CompressionType.BZIP2
    elif "snappy" in compression_magic_type:
        # TODO: This will likely not work - need to handle snappy & zlib separately
        return CompressionType.SNAPPY
    else:
        return CompressionType.NONE


def collect_payload_from_s3(
    agent_config, bucket_name, object_key, s3_handler, header_byte_length=4096
):
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
                    "key_val_partition_separator", DEFAULT_PARTITION_SEPARATOR
                )
                partition_fields_in_data = match_lariat_file_partition_pattern(
                    suffix_key,
                    bucket_config.get("suffix_template"),
                    partition_separator,
                )
                if partition_fields_in_data:
                    # Retrieve data
                    overall_response = s3_handler.get_object(
                        Bucket=bucket_name, Key=object_key
                    )
                    content_length = overall_response["ContentLength"]
                    overall_response = None
                    header_response = s3_handler.get_object(
                        Bucket=bucket_name,
                        Key=object_key,
                        Range=f"bytes=0-{header_byte_length-1}",
                    )
                    header_bytes = header_response["Body"].read(header_byte_length)
                    file_type = bucket_config.get("file_type")
                    config_name = bucket_config.get("name")
                    mime = magic.Magic(mime=True)
                    compression_magic_type = mime.from_buffer(header_bytes)
                    compression = get_compression_from_magic_type(
                        compression_magic_type
                    )
                    fsspec_name = f"s3://{bucket_name}/{object_key}"
                    if is_content_too_large(content_length):
                        clean_schema = get_schema_for_data(
                            fsspec_name,
                            file_type,
                            partition_fields_in_data,
                        )
                    else:
                        clean_schema = get_schema_for_data(
                            fsspec_name,
                            file_type,
                            partition_fields_in_data,
                            chunksize=None,
                        )
                    return (
                        file_type,
                        fsspec_name,
                        compression,
                        clean_schema,
                        config_name,
                        partition_fields_in_data,
                        content_length,
                    )
    return None, None, None, None, None, None, None


def collect_payload_from_gcs(
    agent_config, bucket_name, object_key, gcs_handler, header_byte_length=4096
):
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
                    "key_val_partition_separator", DEFAULT_PARTITION_SEPARATOR
                )
                partition_fields_in_data = match_lariat_file_partition_pattern(
                    suffix_key,
                    bucket_config.get("suffix_template"),
                    partition_separator,
                )
                if partition_fields_in_data:
                    # Retrieve data
                    gcs_bucket = gcs_handler.get_bucket(bucket_name)
                    blob = gcs_bucket.get_blob(object_key)
                    content_length = blob.size
                    header_bytes = blob.download_as_bytes(
                        start=0, end=header_byte_length - 1
                    )
                    file_type = bucket_config.get("file_type")
                    config_name = bucket_config.get("name")
                    mime = magic.Magic(mime=True)
                    compression_magic_type = mime.from_buffer(header_bytes)
                    compression = get_compression_from_magic_type(
                        compression_magic_type
                    )
                    fsspec_name = f"gcs://{bucket_name}/{object_key}"
                    if is_content_too_large(content_length):
                        clean_schema = get_schema_for_data(
                            fsspec_name,
                            file_type,
                            partition_fields_in_data,
                        )
                    else:
                        clean_schema = get_schema_for_data(
                            fsspec_name,
                            file_type,
                            partition_fields_in_data,
                            chunksize=None,
                        )
                    return (
                        file_type,
                        fsspec_name,
                        compression,
                        clean_schema,
                        config_name,
                        partition_fields_in_data,
                        content_length,
                    )
    return None, None, None, None, None, None, None
