from lariat_agents.base.base_agent import BaseAgent
from lariat_agents.constants import (
    LARIAT_INDICATOR_URL,
    BACKFILL_LARIAT_INDICATOR_URL,
    LARIAT_SCHEMA_URL,
)
from lariat_agents.agent.snowflake.snowflake_query_builder import SnowflakeQueryBuilder
from boto3_type_annotations import s3
import os


BACKFILL_BATCH_AGENT_QUERY_DISPATCH_MODE = "backfill_batch_agent_query_dispatch"
BATCH_AGENT_QUERY_DISPATCH_MODE = "batch_agent_query_dispatch"
SCHEMA_RETRIEVAL_MODE = "raw_schema"
LARIAT_UDF_DB = os.getenv("LARIAT_META_DB", '"lariat_meta_db"')
LARIAT_UDF_SCHEMA = os.getenv("LARIAT_META_SCHEMA", '"lariat"')


class SnowflakeAgent(BaseAgent):
    def __init__(
        self,
        agent_type: str,
        cloud: str,
        s3_handler: s3.Client,
        api_key: str = None,
        application_key: str = None,
    ):
        self.s3_handler = s3_handler
        query_builder = SnowflakeQueryBuilder(
            query_builder_type=agent_type,
            s3_handler=self.s3_handler,
            lariat_udf_db=LARIAT_UDF_DB,
            lariat_udf_schema=LARIAT_UDF_SCHEMA,
        )
        super().__init__(
            agent_type=agent_type,
            cloud=cloud,
            api_key=api_key,
            application_key=application_key,
            query_builder=query_builder,
        )

    def schema_retrieval(self):
        output_schema_list = []
        agent_config = self.get_yaml_config()
        source_id = agent_config["source_id"]
        for database, schemas in agent_config["databases"].items():
            for schema, tables in schemas.items():
                output_schema_list.extend(
                    self.query_builder.run_schema_retrieval(
                        table_schema=schema,
                        table_names=tables,
                        source_id=source_id,
                        db_name=database,
                    )
                )
        self.send_payload_to_agent(
            payload=output_schema_list, endpoint=LARIAT_SCHEMA_URL
        )

    def map_action_to_function(self, action, event_dict=None):
        """
        Supported actions:
        - backfill_batch_agent_query_dispatch: request indicators for a backfill and run them
        - batch_agent_query_dispatch: request the next current indicators and run them
        - raw_schema: request the raw_schema based on the tables and schemas specified in the config yaml
        :param action: One of the supported actions for the agent to run
        :param event_dict: Any additional event specific data
        :return:
        """
        if action is None:
            raise ValueError(
                "Unspecified action. Please consult the docs to pass in the correct action to the agent"
            )
        if action == BACKFILL_BATCH_AGENT_QUERY_DISPATCH_MODE:
            backfill_indicators = self.get_lariat_indicator_json(
                BACKFILL_LARIAT_INDICATOR_URL
            )
            self.execute_indicators(
                indicators=backfill_indicators,
                expect_results=True,
                sketch_type_in_hash=True,
            )
        elif action == BATCH_AGENT_QUERY_DISPATCH_MODE:
            indicators = self.get_lariat_indicator_json(LARIAT_INDICATOR_URL)
            self.execute_indicators(
                indicators=indicators, expect_results=True, sketch_type_in_hash=True
            )
        elif action == SCHEMA_RETRIEVAL_MODE:
            self.schema_retrieval()
        else:
            raise ValueError(f"Invalid Action Specified {action}")
