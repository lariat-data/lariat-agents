import pytest
import lariat_python_common.athena.schema as lariat_athena_schema
from lariat_python_common.test.utils import data_loader, get_test_labels
from lariat_python_common.athena.schema import get_schema_retrieval_query
from lariat_python_common.athena.tests.data.schema.test_cases import tests


@pytest.mark.parametrize(
    "unparsed_schema,expected",
    [
        (
            "col_name row(something varchar)",
            [{"data_type": "row(something varchar)", "column_name": "col_name"}],
        ),
        (
            "statement varchar, testing row(something array(varchar))",
            [
                {"column_name": "statement", "data_type": "varchar"},
                {
                    "column_name": "testing",
                    "data_type": "row(something array(varchar))",
                },
            ],
        ),
    ],
    ids=["Easy_Case_0", "Easy_Case_1"],
)
def test_get_schema_from_type(unparsed_schema, expected):
    actual = lariat_athena_schema.get_schema_from_type(unparsed_schema)
    assert actual == expected


@pytest.mark.parametrize(
    "test_information_schema,expected",
    data_loader(
        tests["transform_athena_to_json_schema_tests"],
        [
            "test_information_schema",
            "expected",
        ],
    ),
    ids=get_test_labels(tests["transform_athena_to_json_schema_tests"]),
)
def test_transform_athena_to_json_schema(test_information_schema, expected):
    actual = lariat_athena_schema.transform_athena_to_json_schema(
        test_information_schema
    )
    assert actual == expected


@pytest.mark.parametrize(
    "table_names,table_schema,expected_query",
    [
        (
            ["v1", "v2", "d*"],
            "test",
            "SELECT table_schema,table_name,column_name,data_type,extra_info FROM information_schema.columns where table_schema='test' and (table_name IN ('v1','v2') OR table_name like 'd%')",
        ),
        (
            ["v1", "v2", "v*"],
            "test",
            "SELECT table_schema,table_name,column_name,data_type,extra_info FROM information_schema.columns where table_schema='test' and (table_name IN ('v1','v2') OR table_name like 'v%')",
        ),
        (
            ["v1", "v2"],
            "test",
            "SELECT table_schema,table_name,column_name,data_type,extra_info FROM information_schema.columns where table_schema='test' and (table_name IN ('v1','v2') )",
        ),
        (
            ["d*"],
            "test",
            "SELECT table_schema,table_name,column_name,data_type,extra_info FROM information_schema.columns where table_schema='test' and (  table_name like 'd%')",
        ),
    ],
    ids=["Easy_Case_0", "Medium_Case_0", "Easy_Case_1", "Easy_Case_2"],
)
def test_get_schema_retrieve_query(table_names, table_schema, expected_query):
    assert get_schema_retrieval_query(table_names, table_schema) == expected_query
