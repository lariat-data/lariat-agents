from lariat_agents.agent.iceberg.iceberg_agent import IcebergAgent

RUN_TYPE_KEY = "run_type"


def lambda_handler(event, context):
    """
    Lambda entry point to invoke batch agent functions.
    :param event: Dictionary that may include RUN_TYPE_KEY from Eventbridge (CloudWatch Events)
        RUN_TYPE_KEY can either be "batch_agent_query_dispatch", "raw_schema" or "batch_agent_copy",
        or "backfill_batch_agent_query_dispatch"
    :param context: Lambda provided data that isn't currently used by downstream code
    :return:
    """
    agent = IcebergAgent(agent_type="iceberg", cloud="aws")
    return agent.map_action_to_function(event[RUN_TYPE_KEY], event)
