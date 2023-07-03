import json
import os

import boto3
import pytest

from lariat_agents.agent.athena.athena_agent import AthenaAgent
from lariat_agents.base.base_agent import BaseAgent
from lariat_agents.base.tests.data.test_cases import tests
from lariat_python_common.test.utils import data_loader, get_test_labels
from moto import mock_athena, mock_s3
from lariat_agents.constants import CLOUD_AGENT_CONFIG_PATH


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test_lariat"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test_lariat"
    os.environ["AWS_SECURITY_TOKEN"] = "test_lariat"
    os.environ["AWS_SESSION_TOKEN"] = "test_lariat"


@mock_athena
@mock_s3
@pytest.mark.parametrize(
    "indicators, expect",
    data_loader(
        tests["execute_indicators"],
        [
            "indicators",
            "expect",
        ],
    ),
    ids=get_test_labels(tests["execute_indicators"]),
)
def test_execute_indicators(aws_credentials, indicators, expect):
    """
    This tests the execute_indicators functionality in BaseAgent.
    """
    test_file_data = json.dumps({"source_id": "source_id1"}).encode()
    athena = boto3.client("athena", region_name="us-east-1")
    s3 = boto3.client("s3", region_name="us-east-1")
    test_bucket_name = CLOUD_AGENT_CONFIG_PATH.split("/")[0]
    test_cloud_config_path = CLOUD_AGENT_CONFIG_PATH[
        CLOUD_AGENT_CONFIG_PATH.index("/") + 1 :
    ]
    s3.create_bucket(Bucket=test_bucket_name)
    s3.put_object(
        Bucket=test_bucket_name,
        Key=test_cloud_config_path,
        Body=test_file_data,
    )
    agent = AthenaAgent(
        agent_type="athena", cloud="aws", athena_handler=athena, s3_handler=s3
    )
    response = agent.execute_indicators(indicators=indicators, expect_results=True)
    assert response == expect


@pytest.mark.parametrize(
    "calculation, expect",
    data_loader(
        tests["get_sketch_type_from_calculation"],
        [
            "calculation",
            "expect",
        ],
    ),
    ids=get_test_labels(tests["get_sketch_type_from_calculation"]),
)
def test_get_sketch_type_from_calculation(calculation, expect):
    """
    This tests the get_sketch_type_from_calculation functionality in BaseAgent.
    """
    response = BaseAgent.get_sketch_type_from_calculation(calculation)
    assert response == expect
