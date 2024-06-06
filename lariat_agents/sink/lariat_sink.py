import pandas as pd
from lariat_agents.constants import (
    CLOUD_TYPE_AWS,
    CLOUD_TYPE_GCP,
    LARIAT_OUTPUT_BUCKET,
    LARIAT_SINK_AWS_ACCESS_KEY_ID,
    LARIAT_SINK_AWS_SECRET_ACCESS_KEY,
    CROSS_ACCOUNT_ROLE_BASE_ARN,
    GCP_CROSS_ACCOUNT_ROLE_BASE_ARN,
    USER_CLOUD_ACCOUNT_ID,
    LARIAT_SINK_CREDENTIALS_REGION_NAME,
    RESULT_OUTPUT_RESULT_MAX_TS,
    RESULT_OUTPUT_RESULT_MIN_TS,
)
from lariat_agents.base.base_sink import BaseSink
import boto3
from typing import Dict
from lariat_python_common.aws import utils as aws_utils
import logging


class LariatSink(BaseSink):
    """
    Sink responsible for sending a timestamp, value, and dimension (when relevant)
    to the Lariat Service for each indicator
    """

    def __init__(
        self,
        source_cloud: str,
    ):
        self.s3_client = None
        self.azure_container_client = None
        if source_cloud == CLOUD_TYPE_AWS:
            self.s3_client = boto3.client("s3")
        super().__init__(source_cloud)

    def write(
        self,
        result_df: pd.DataFrame = None,
        source_top_level: str = None,
        file_path: str = None,
        tags_dict: Dict = None,
    ):
        """
        The write function supports either writing result_df to the file path in LARIAT_OUTPUT_BUCKET indicated
        OR copying data from the source_top_level and file_path to the file_path in LARIAT_OUTPUT_BUCKET.
        If a source_top_level and file_path are passed in, the result_df is ignored.

        This sink doesn't support tags.
        """
        if self.source_cloud == CLOUD_TYPE_GCP:
            role_arn_prefix = GCP_CROSS_ACCOUNT_ROLE_BASE_ARN
        else:
            role_arn_prefix = CROSS_ACCOUNT_ROLE_BASE_ARN
        try:
            (
                access_key_id,
                secret_access_key,
                session_token,
            ) = aws_utils.get_and_decrypt_keypair(
                aws_access_key_id=LARIAT_SINK_AWS_ACCESS_KEY_ID,
                aws_secret_access_key=LARIAT_SINK_AWS_SECRET_ACCESS_KEY,
                customer_account_id=USER_CLOUD_ACCOUNT_ID,
                role_arn_prefix=role_arn_prefix,
                region_name=LARIAT_SINK_CREDENTIALS_REGION_NAME,
            )
            if source_top_level and file_path:
                if self.source_cloud == CLOUD_TYPE_AWS:
                    athena_source_location = {
                        "Bucket": source_top_level,
                        "Key": file_path,
                    }
                    session = boto3.Session(
                        aws_access_key_id=access_key_id,
                        aws_secret_access_key=secret_access_key,
                        aws_session_token=session_token,
                    )
                    s3_resource = session.resource("s3")
                    bucket = s3_resource.Bucket(LARIAT_OUTPUT_BUCKET)
                    bucket.copy(athena_source_location, file_path)
            elif result_df is not None:
                if result_df.empty:
                    logging.warning("Empty Result Set: No Indicators Written")
                else:
                    if RESULT_OUTPUT_RESULT_MAX_TS in result_df.columns:
                        result_df = result_df[
                            ~result_df[RESULT_OUTPUT_RESULT_MAX_TS].isnull()
                        ]
                    if RESULT_OUTPUT_RESULT_MIN_TS in result_df.columns:
                        result_df = result_df[
                            ~result_df[RESULT_OUTPUT_RESULT_MIN_TS].isnull()
                        ]
                    if result_df.empty:
                        logging.warning(
                            "No data available for indicators: No Indicators Written"
                        )
                        return
                    result_df.to_csv(
                        f"s3://{LARIAT_OUTPUT_BUCKET}/{file_path}",
                        index=False,
                        storage_options={
                            "key": access_key_id,
                            "secret": secret_access_key,
                            "token": session_token,
                        },
                    )

        except Exception as e:
            logging.error(f"Failed to write data via Lariat sink. {e}")
