from lariat_agents.agent.event_payload.event_payload_agent import EventPayloadAgent
import base64
import json
from google.cloud import storage
from fastapi import FastAPI, Request

app = FastAPI()


@app.post("/")
async def run_agent_gcp(request: Request):
    envelope = await request.json()
    if not envelope:
        return {"error": "Bad Request: no JSON body"}

    # Extract the event data
    event = envelope.get("message", {}).get("data")
    if not event:
        return {"error": "Invalid Request: Missing Event Data"}
    decoded_event_data = base64.b64decode(event).decode("utf-8")
    event = json.loads(decoded_event_data)
    gcs_handler = storage.Client()
    agent = EventPayloadAgent(
        agent_type="event_payload", cloud="gcp", gcs_handler=gcs_handler
    )
    return agent.map_action_to_function(None, event)
