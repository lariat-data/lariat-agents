from enum import Enum


class CloudTypeModes(Enum):
    AZURE = "azure"
    AWS = "aws"
    GCP = "gcp"
    NONE = "local"


class SketchTypeModes(Enum):
    DISTINCT = "COUNT_DISTINCT"
    DECILE = "DECILE"
    NONE = "NONE"
