from lariat_agents.base.base_agent import BaseAgent
from lariat_agents.constants import (
    LARIAT_INDICATOR_URL,
    BACKFILL_LARIAT_INDICATOR_URL,
    LARIAT_SCHEMA_URL,
)
from athena_query_builder import AthenaQueryBuilder
import os
from boto3_type_annotations import athena, s3
from typing import Dict
from s3path import S3Path
import lariat_python_common.athena.utils as athena_utils


BACKFILL_BATCH_AGENT_QUERY_DISPATCH_MODE = "backfill_batch_agent_query_dispatch"
BATCH_AGENT_QUERY_DISPATCH_MODE = "batch_agent_query_dispatch"
BATCH_AGENT_COPY_MODE = "batch_agent_copy"
SCHEMA_RETRIEVAL_MODE = "raw_schema"
WORKGROUP_FILTER = os.getenv("LARIAT_ATHENA_WORKGROUP", "primary")
DEFAULT_DATABASE_NAME = "default"
ATHENA_QUERY_BUCKET_NAME = os.getenv(
    "S3_QUERY_RESULTS_BUCKET", "lariat-athena-monitoring-output-test"
)
ATHENA_STATE_CHANGE = "Athena Query State Change"
CLOUDWATCH_ATHENA_EVENT_DETAIL_TYPE = "detail-type"


class AthenaAgent(BaseAgent):
    def __init__(
        self,
        agent_type: str,
        cloud: str,
        athena_handler: athena.Client,
        s3_handler: s3.Client,
        api_key: str = None,
        application_key: str = None,
    ):
        self.s3_handler = s3_handler
        self.athena_handler = athena_handler
        query_builder = AthenaQueryBuilder(
            query_builder_type=agent_type,
            workgroup_name=WORKGROUP_FILTER,
            athena_query_bucket_name=ATHENA_QUERY_BUCKET_NAME,
            database_name=DEFAULT_DATABASE_NAME,
            athena_handler=self.athena_handler,
            s3_handler=self.s3_handler,
        )
        super().__init__(
            agent_type=agent_type,
            cloud=cloud,
            api_key=api_key,
            application_key=application_key,
            query_builder=query_builder,
        )

    def setup_and_execute_write(self, event_dict: Dict):
        """
        This function is called when the signal that a query has finished is received by the agent.
        It then ensures that the data is written to the sink by calling the write function.
        :param event_dict:
        :return:
        """
        if event_dict[CLOUDWATCH_ATHENA_EVENT_DETAIL_TYPE] == ATHENA_STATE_CHANGE:
            if "previousState" in event_dict["detail"]:
                query_execution_id = event_dict["detail"]["queryExecutionId"]
                current_state = event_dict["detail"]["currentState"]
                previous_state = event_dict["detail"]["previousState"]
                workgroup = event_dict["detail"]["workgroupName"]
                if (
                    current_state == "SUCCEEDED"
                    and previous_state == "RUNNING"
                    and workgroup == WORKGROUP_FILTER
                ):
                    response = self.athena_handler.get_query_execution(
                        QueryExecutionId=query_execution_id
                    )

                    result_output_path = response["QueryExecution"][
                        "ResultConfiguration"
                    ]["OutputLocation"].removeprefix("s3:/")
                    output_s3path_obj = S3Path(f"{result_output_path}")
                    query, meta = athena_utils.get_meta_and_query_from_execution_id(
                        query_execution_id=query_execution_id,
                        error_state=False,
                        athena_handler=self.athena_handler,
                    )
                    indicator_statuses = (
                        self.query_builder.construct_indicator_statuses_from_meta(
                            query=query, meta_dict=meta
                        )
                    )
                    self.write_data(
                        source_file_path=output_s3path_obj.key,
                        source_top_level=output_s3path_obj.bucket,
                        indicator_statuses=indicator_statuses,
                    )

    def schema_retrieval(self):
        output_schema_list = []
        agent_config = self.get_yaml_config()
        source_id = agent_config["source_id"]
        for database, tables in agent_config["databases"].items():
            output_schema_list.extend(
                self.query_builder.run_schema_retrieval(
                    table_schema=database,
                    table_names=tables,
                    source_id=source_id,
                )
            )
        self.send_payload_to_agent(
            payload=output_schema_list, endpoint=LARIAT_SCHEMA_URL
        )

    def map_action_to_function(self, action, event_dict=None):
        if not event_dict:
            event_dict = {}
        if action is None:
            action = BATCH_AGENT_QUERY_DISPATCH_MODE
        if action == BACKFILL_BATCH_AGENT_QUERY_DISPATCH_MODE:
            backfill_indicators = self.get_lariat_indicator_json(
                BACKFILL_LARIAT_INDICATOR_URL
            )
            self.execute_indicators(
                indicators=backfill_indicators, expect_results=False
            )
        elif action == BATCH_AGENT_QUERY_DISPATCH_MODE:
            indicators = self.get_lariat_indicator_json(LARIAT_INDICATOR_URL)
            self.execute_indicators(indicators=indicators, expect_results=False)
        elif action == BATCH_AGENT_COPY_MODE:
            self.setup_and_execute_write(event_dict)
        elif action == SCHEMA_RETRIEVAL_MODE:
            self.schema_retrieval()
