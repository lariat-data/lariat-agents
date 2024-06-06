from lariat_agents.agent.event_payload.event_payload_agent import EventPayloadAgent
from google.cloud import storage
from lariat_agents.constants import GCS_INPUT_BUCKET_VAR_NAME, GCS_INPUT_OBJECT_VAR_NAME
import os


def run_agent_gcp():
    input_bucket = os.getenv(GCS_INPUT_BUCKET_VAR_NAME)
    input_object = os.getenv(GCS_INPUT_OBJECT_VAR_NAME)

    if not (input_bucket or input_object):
        raise ValueError("Input Event doesn't contain a bucket or an object")

    # Extract the event data
    event = {"bucket": input_bucket, "name": input_object}
    if not event:
        return {"error": "Bad Request: missing event data"}

    gcs_handler = storage.Client()
    agent = EventPayloadAgent(
        agent_type="event_payload", cloud="gcp", gcs_handler=gcs_handler
    )
    return agent.map_action_to_function(None, event)


if __name__ == "__main__":
    run_agent_gcp()
