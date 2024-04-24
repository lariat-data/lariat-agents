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
    agent = EventPayloadAgent(agent_type="event_payload", cloud="aws", s3_handler=s3)
    return agent.map_action_to_function(None, event)
