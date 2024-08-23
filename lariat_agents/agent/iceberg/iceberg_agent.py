from lariat_agents.base.batch_base.batch_base_agent import BatchBaseAgent
from lariat_agents.constants import (
    LARIAT_INDICATOR_URL,
    LARIAT_SCHEMA_URL,
    BACKFILL_LARIAT_INDICATOR_URL,
)
from lariat_agents.agent.iceberg.iceberg_query_builder import IcebergQueryBuilder
from lariat_agents.agent.iceberg.iceberg_agent_data_types import CatalogType
from pyiceberg.catalog import load_catalog

BATCH_AGENT_QUERY_DISPATCH_MODE = "batch_agent_query_dispatch"
SCHEMA_RETRIEVAL_MODE = "raw_schema"

import logging
from sqlalchemy.engine.url import URL


class IcebergAgent(BatchBaseAgent):
    def __init__(
        self,
        agent_type: str,
        cloud: str,
        api_key: str = None,
        application_key: str = None,
    ):

        query_builder = IcebergQueryBuilder(
            query_builder_type=agent_type, sketch_mode=False
        )
        super().__init__(
            agent_type=agent_type,
            cloud=cloud,
            api_key=api_key,
            application_key=application_key,
            query_builder=query_builder,
        )
        catalogs = {}
        agent_config = self.yaml_config["catalog"]
        dataset_to_catalog_map = {}
        for catalog in agent_config:
            if catalog in CatalogType._value2member_map_:
                if CatalogType(catalog) == CatalogType.GLUE:
                    """
                    "s3.access-key-id": "access-key", #s3. service specific. you can also do glue.
                    "s3.secret-access-key": "secret-access-key",
                    "s3.region": "us-east-1"
                    "client.access-key-id": "access-key", # client. for everything
                    "client.secret-access-key": "secret-access-key",
                    "client.region": "us-east-1"
                    """
                    catalogs[catalog] = load_catalog("glue", **{"type": "glue"})
                    for db in agent_config[catalog]["databases"]:
                        all_table_names = [
                            key
                            for item in agent_config[catalog]["databases"][db]
                            for key in item
                        ]
                        for table in all_table_names:
                            dataset_to_catalog_map[f"{catalog}.{db}.{table}"] = catalog
                elif CatalogType(catalog) == CatalogType.POSTGRES:
                    db_config_dict = agent_config[catalog]
                    driver = "postgresql+psycopg2"
                    user = db_config_dict.get("user", None)
                    pwd = db_config_dict.get("password", None)
                    host = db_config_dict.get("host", None)
                    port = db_config_dict.get("port", None)
                    uri = URL.create(
                        drivername=driver,
                        username=user,
                        password=pwd,
                        host=host,
                        port=port,
                    ).render_as_string(hide_password=False)
                    catalogs[catalog] = load_catalog(
                        "sql", **{"type": "sql", "uri": uri}
                    )
                elif CatalogType(catalog) == CatalogType.SQLLITE:
                    db_config_dict = agent_config[catalog]
                    driver = "sqllite:///"
                    path = db_config_dict.get("db_path", None)
                    uri = f"{driver}{path}"
                    catalogs[catalog] = load_catalog(
                        "sql", **{"type": "sql", "uri": uri}
                    )
                else:
                    continue
            else:
                logging.warning(f"Catalog Type {catalog} not supported")
        query_builder.dataset_to_catalog_map = dataset_to_catalog_map
        query_builder.catalogs = catalogs
        self.catalogs = catalogs

    def schema_retrieval(self, event_dict=None):
        output_schema_list = []
        agent_config = self.yaml_config
        source_id = agent_config["source_id"]
        for catalog in agent_config["catalog"]:
            if catalog in CatalogType._value2member_map_:
                for database, tables in agent_config["catalog"][catalog][
                    "databases"
                ].items():
                    all_table_names = [key for item in tables for key in item]
                    output_schema_list.extend(
                        self.query_builder.run_schema_retrieval(
                            table_schema=database,
                            table_names=all_table_names,
                            source_id=source_id + "_" + catalog,
                            catalog=self.catalogs[catalog],
                        )
                    )
            else:
                raise ValueError("Incorrect value: {catalog} passed into config")
        if output_schema_list:
            self.send_payload_to_agent(
                payload=output_schema_list, endpoint=LARIAT_SCHEMA_URL
            )
        return output_schema_list

    def map_action_to_function(self, action, event_dict=None):
        """
        Supported actions:
        - backfill_batch_agent_query_dispatch: Dispatch async queries for the next set of indicators marked for backfill
        - batch_agent_query_dispatch: Dispatch async queries for next current set of indicators to run
        - batch_agent_copy: Copy Data from async query execution
        - raw_schema: request the raw_schema based on the tables and schemas specified in the config yaml
        :param action: One of the supported actions for the agent to run
        :param event_dict: Any additional event specific data (e.g. data about async executions)
        :return:
        """
        if not event_dict:
            event_dict = {}
        if action is None:
            raise ValueError(
                "Unspecified action. Please consult the docs to pass in the correct action to the agent"
            )
        if action == BATCH_AGENT_QUERY_DISPATCH_MODE:
            indicators = self.get_lariat_indicator_json(LARIAT_INDICATOR_URL)
            if indicators is None or indicators.empty:
                indicators = self.get_lariat_indicator_json(
                    BACKFILL_LARIAT_INDICATOR_URL
                )
            if not (indicators is None or indicators.empty):
                self.execute_indicators(indicators=indicators, expect_results=True)
        elif action == SCHEMA_RETRIEVAL_MODE:
            output = self.schema_retrieval()
            logging.warning(f"Lambda Output: {output}")
            return output
        else:
            raise ValueError(f"Invalid Action Specified {action}")
