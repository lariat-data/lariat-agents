from pydantic import BaseModel
from typing import Optional, Dict
from enum import Enum


class EventType(Enum):
    SNS_S3_TRIGGER = "sns_s3_trigger"
    S3_TRIGGER = "s3_trigger"
    GCS_TRIGGER = "gcs_trigger"
    LAMBDA_DESTINATION_S3_TRIGGER = "lambda_destination_s3_trigger"
    LAMBDA_DESTINATION_SNS_S3_TRIGGER = "lambda_destination_sns_s3_trigger"


class PayloadSource(Enum):
    S3 = "s3"
    GCS = "gcs"


class SupportedPayloadFormat(Enum):
    JSONL = "jsonl"
    JSON = "json"
    PARQUET = "parquet"
    CSV = "csv"
    AVRO = "avro"


class EventPayload(BaseModel):
    event_source: EventType
    bucket: Optional[str] = None
    object_key: str
    payload_source: PayloadSource
    raw_event: Dict


class CompressionType(Enum):
    GZIP = "gzip"
    BZIP2 = "bz2"
    SNAPPY = "snappy"
    NONE = None
