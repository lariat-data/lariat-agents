import boto3
from azure.storage.blob import (
    BlobServiceClient,
)
from io import StringIO
from lariat_python_common.types.types import CloudTypeModes

CLOUD_TYPE_AWS = CloudTypeModes.AWS.value
CLOUD_TYPE_AZURE = CloudTypeModes.AZURE.value
CLOUD_TYPE_NONE = CloudTypeModes.NONE.value

# TODO: Switch cloud arguments in below functions from str to CloudTypeModes


def read_stringio_from_cloud(path_elements, cloud):
    if cloud == CLOUD_TYPE_AWS:
        bucket, key = path_elements
        s3_handler = boto3.client("s3")
        response = s3_handler.get_object(Bucket=bucket, Key=key)
        return StringIO(response["Body"].read().decode("utf-8"))
    elif cloud == CLOUD_TYPE_AZURE:
        (
            blob_path,
            azure_storage_container,
            azure_storage_connection_string,
        ) = path_elements
        blob_service_client = BlobServiceClient.from_connection_string(
            azure_storage_connection_string
        )
        container_client = blob_service_client.get_container_client(
            container=azure_storage_container
        )
        return StringIO(
            container_client.download_blob(blob_path).readall().decode("utf-8")
        )
    elif cloud == CLOUD_TYPE_NONE:
        StringIO(path_elements)
        with open(path_elements) as file_object:
            return StringIO(file_object.read())


def write_stringio_to_cloud(string_io_buffer, path_elements, cloud):
    if cloud == CLOUD_TYPE_AWS:
        bucket, key = path_elements
        s3_handler = boto3.client("s3")
        s3_handler.put_object(Bucket=bucket, Key=key, Body=string_io_buffer.getvalue())
    elif cloud == CLOUD_TYPE_AZURE:
        (
            blob_path,
            azure_storage_container,
            azure_storage_connection_string,
        ) = path_elements
        blob_service_client = BlobServiceClient.from_connection_string(
            azure_storage_connection_string
        )
        blob_client = blob_service_client.get_blob_client(
            container=azure_storage_container, blob=blob_path
        )
        blob_client.upload_blob(string_io_buffer)
    elif cloud == CLOUD_TYPE_NONE:
        with open(path_elements) as file_object:
            return file_object
