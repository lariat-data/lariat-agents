import boto3
from lariat_agents.agent.event_payload.event_payload_agent import EventPayloadAgent
import logging
import json


def lambda_handler(event, context):
    """
    Lambda entry point to invoke agent functions.
    :param event: S3 trigger event.
    :param context: Lambda provided data that isn't currently used by downstream code
    :return:
    """
    s3 = boto3.client("s3")
    logging.warning(f"{json.dumps({'input_event': event})}")
    agent = EventPayloadAgent(agent_type="event_payload", cloud="local", s3_handler=s3)
    return agent.map_action_to_function(None, event)


if __name__ == "__main__":
    lambda_handler(
        json.loads(
            """{
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2024-05-17T17:11:20.069Z",
                "eventName": "ObjectCreated:Copy",
                "userIdentity": {
                    "principalId": "AWS:AROAVHAY2GCN3DRUF2O3S:s3-session-619038855486"
                },
                "requestParameters": {
                    "sourceIPAddress": "44.223.39.45"
                },
                "responseElements": {
                    "x-amz-request-id": "FW2BYCAH4RZS0H8S",
                    "x-amz-id-2": "VULvioM4RGbdfgraktoCIJLIQI9NfZSlZecpfe/Qz+nfdSt3gDGR1XJ+WaquLlURTQprj60tuIXEpNQRJIkUZbBlA/axJAuZ"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "batch-agent-ingest",
                    "bucket": {
                        "name": "lariat-batch-agent-sink",
                        "ownerIdentity": {
                            "principalId": "A7X2P2SUIUN75"
                        },
                        "arn": "arn:aws:s3:::lariat-batch-agent-sink"
                    },
                    "object": {
                        "key": "overturemaps/release/2024-05-16-beta.0/theme=places/type=place/part-00000-1c61c41d-dae5-455d-a7ae-e7bf293ffccd-c000.zstd.parquet",
                        "size": 175,
                        "eTag": "6aa051ffdd7134344979ac295bb49c23",
                        "sequencer": "0066478FB7F0B30D92"
                    }
                }
            }
        ]
    }"""
        ),
        None,
    )
# "New_Feed/2024/02/11/t_w_20240423_part0017071601.csv.gz",
# "New_Feed/2024/02/12/one_million.csv.gz"
