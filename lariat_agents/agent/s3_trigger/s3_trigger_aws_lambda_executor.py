import boto3
from lariat_agents.agent.s3_trigger.s3_trigger_agent import S3TriggerAgent


def lambda_handler(event, context):
    """
    Lambda entry point to invoke agent functions.
    :param event: S3 trigger event.
    :param context: Lambda provided data that isn't currently used by downstream code
    :return:
    """
    s3 = boto3.client("s3")
    agent = S3TriggerAgent(agent_type="s3_trigger", cloud="aws", s3_handler=s3)
    return agent.map_action_to_function(None, event)

