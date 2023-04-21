from lariat_python_common.test.utils import TestComplexity, TEST_COMPLEXITY_KEY

tests = {
    "transform_athena_to_json_schema_tests": {
        "simple_case_0": {
            "test_information_schema": [
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "deviceid",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "devicetype",
                    "data_type": "row(value varchar)",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "eventtimemilli",
                    "data_type": "bigint",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "eventdate",
                    "data_type": "date",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "location",
                    "data_type": "row(latitude double, longitude double)",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "countrycode",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "horizontalaccuracy",
                    "data_type": "double",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "altitude",
                    "data_type": "double",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "verticalaccuracy",
                    "data_type": "double",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "bearing",
                    "data_type": "double",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "speed",
                    "data_type": "double",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "devicecarrier",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "ipaddressv4",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "ipaddressv6",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "metadata",
                    "data_type": "map(varchar, varchar)",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "gdprconsentflag",
                    "data_type": "boolean",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "partition_0",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "receiveddate",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "receivedhour",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "partnerid",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "sourcebatchdate",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "table_1",
                    "table_name": "table_name_1",
                    "column_name": "additional_int",
                    "data_type": "integer",
                },
            ],
            "expected": {
                "type": "object",
                "properties": {
                    "deviceid": {"type": "string"},
                    "devicetype": {
                        "type": "object",
                        "properties": {"value": {"type": "string"}},
                    },
                    "eventtimemilli": {
                        "type": "integer",
                        "format": "bigint",
                    },
                    "eventdate": {"type": "string", "format": "date"},
                    "location": {
                        "type": "object",
                        "properties": {
                            "latitude": {"type": "number", "format": "double"},
                            "longitude": {"type": "number", "format": "double"},
                        },
                    },
                    "countrycode": {"type": "string"},
                    "horizontalaccuracy": {"type": "number", "format": "double"},
                    "altitude": {"type": "number", "format": "double"},
                    "verticalaccuracy": {"type": "number", "format": "double"},
                    "bearing": {"type": "number", "format": "double"},
                    "speed": {"type": "number", "format": "double"},
                    "devicecarrier": {"type": "string"},
                    "ipaddressv4": {"type": "string"},
                    "ipaddressv6": {"type": "string"},
                    "metadata": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                    },
                    "gdprconsentflag": {"type": "boolean"},
                    "partition_0": {"type": "string"},
                    "receiveddate": {"type": "string"},
                    "receivedhour": {"type": "string"},
                    "partnerid": {"type": "string"},
                    "sourcebatchdate": {"type": "string"},
                    "additional_int": {"type": "integer", "format": "int"},
                },
            },
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_1": {
            "test_information_schema": [
                {
                    "table_schema": "client_db_anonymized_21",
                    "table_name": "dataset_22",
                    "column_name": "body",
                    "data_type": "row(statement varchar, testing row(something array(varchar)))",
                },
                {
                    "table_schema": "client_db_anonymized_21",
                    "table_name": "dataset_22",
                    "column_name": "event_id",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "client_db_anonymized_21",
                    "table_name": "dataset_22",
                    "column_name": "name",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "client_db_anonymized_21",
                    "table_name": "dataset_22",
                    "column_name": "org_id",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "client_db_anonymized_21",
                    "table_name": "dataset_22",
                    "column_name": "timestamp",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "client_db_anonymized_21",
                    "table_name": "dataset_22",
                    "column_name": "year",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "client_db_anonymized_21",
                    "table_name": "dataset_22",
                    "column_name": "month",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "client_db_anonymized_21",
                    "table_name": "dataset_22",
                    "column_name": "day",
                    "data_type": "varchar",
                },
                {
                    "table_schema": "client_db_anonymized_21",
                    "table_name": "dataset_22",
                    "column_name": "hour",
                    "data_type": "varchar",
                },
            ],
            "expected": {
                "type": "object",
                "properties": {
                    "body": {
                        "type": "object",
                        "properties": {
                            "statement": {"type": "string"},
                            "testing": {
                                "type": "object",
                                "properties": {
                                    "something": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    }
                                },
                            },
                        },
                    },
                    "event_id": {"type": "string"},
                    "name": {"type": "string"},
                    "org_id": {"type": "string"},
                    "timestamp": {"type": "string"},
                    "year": {"type": "string"},
                    "month": {"type": "string"},
                    "day": {"type": "string"},
                    "hour": {"type": "string"},
                },
            },
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
    },
}
