from lariat_agents.agent.event_payload.event_payload_agent import EventPayloadAgent
import base64
import json
from google.cloud import storage

from lariat_agents.constants import GCS_RAW_EVENT_VAR_NAME
import os


def run_agent_gcp():
    """
    Accepts an Object Finalized Event passed in via an Environment Var.
    Decodes and Passes this along to EventPayloadAgent to compute health metrics on the object
    and sink to chosen option (usually Lariat) 
    :return:
    """
    raw_event = os.getenv(GCS_RAW_EVENT_VAR_NAME)

    if not raw_event:
        raise ValueError("Input Event doesn't contain a trigger event")

    # Decode the event data (it's base64 encoded)
    decoded_event_data = base64.b64decode(raw_event).decode("utf-8")
    event = json.loads(decoded_event_data)

    gcs_handler = storage.Client()
    agent = EventPayloadAgent(
        agent_type="event_payload", cloud="gcp", gcs_handler=gcs_handler
    )
    return agent.map_action_to_function(None, event)


if __name__ == "__main__":
    run_agent_gcp()
