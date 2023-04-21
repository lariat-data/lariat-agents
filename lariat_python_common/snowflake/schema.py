"""
    All code related to interpreting and transforming Athena Schemata
"""
from typing import List
import lariat_python_common.string.utils as lariat_string_utils

"""
    Canonical Mapping of Athena types to JSON types and formats
"""
SQL_TYPE_TO_JSON_TYPE = {
    "text": ("string", None),
    "number": ("number", None),
    "varchar": ("string", None),
    "date": ("string", "date"),
    "time": ("string", "time"),
    "double": ("number", "double"),
    "float": ("number", "float"),
    "decimal": ("number", "decimal"),
    "integer": ("integer", "int"),
    "bigint": ("integer", "bigint"),
    "timestamp": ("integer", "date-time"),
    "boolean": ("boolean", None),
    "array": ("array", None),
    "row": ("object", None),
    "map": ("object", None),
    "smallint": ("integer", "smallint"),
    "tinyint": ("integer", "tinyint"),
}


def get_schema_retrieval_query(table_names, table_schema):
    """
    Given list of table names and table schema, it returns the needed query to run against snowflake to
    get these table info, it also support the '*' wild card
    example
    table_names = ['v', 'v*'], table_schema=xxx
    result:
    "SELECT table_schema,table_name,column_name,data_type FROM information_schema.columns where table_schema=xxx
    and table_name IN ('v') amd table_name like 'v%'"
    """
    fully_qualified_table_names = []
    wildcard_table_names = []

    for table_name in table_names:
        if table_name and "*" in table_name:
            wildcard_table_names.append(table_name.replace("*", "%"))
        else:
            fully_qualified_table_names.append(table_name)

    if fully_qualified_table_names:
        formatted_fully_qualfied_table_names = (
            "table_name IN ("
            + ",".join(f"'{table}'" for table in fully_qualified_table_names)
            + ")"
        )
    else:
        formatted_fully_qualfied_table_names = ""

    if wildcard_table_names:
        if formatted_fully_qualfied_table_names:
            wildcard_prefix = "OR"
        else:
            wildcard_prefix = ""
        formatted_wild_card_table_names = (
            f"{wildcard_prefix} table_name like "
            + " OR table_name like ".join(
                f"'{wildcard_table}'" for wildcard_table in wildcard_table_names
            )
        )
    else:
        formatted_wild_card_table_names = ""
    schema_query = (
        f"select table_name, column_name, data_type, is_nullable, character_maximum_length from "
        f"information_schema.columns where table_schema = '{table_schema}'"
    )
    if formatted_fully_qualfied_table_names or formatted_wild_card_table_names:
        schema_query = f"{schema_query} and ({formatted_fully_qualfied_table_names} {formatted_wild_card_table_names})"

    return schema_query


def get_schema_from_type(unparsed_schema: str):
    """
    This takes an unparsed schema string representing a column and parses it into a column name and data_type or into
    just a data_type (if no column name exists).

    Column Names and Data Types are separated by Spaces
    Multipe types are separated by commas
    :param unparsed_schema: A string such as testing row(something array(varchar))
    :return: A dictionary of column name and data_type or just a dictionary of data_type:
            e.g: {"column_name": "testing", "data_type": "row(something array(varchar))"}
    """
    output_schema = []
    for item in lariat_string_utils.tokenize(unparsed_schema):
        tokenized_item = lariat_string_utils.tokenize(
            string=item.strip(), separator=" "
        )
        if len(tokenized_item) > 1:
            column_name, data_type = tokenized_item
            output_schema.append({"column_name": column_name, "data_type": data_type})
        else:
            output_schema.append({"data_type": tokenized_item[0]})
    return output_schema


def transform_snowflake_to_json_schema(
    information_schema: List, json_schema=None, input_node: str = "properties"
):
    """
    Given a list rows from the information schema table, construct the representative json schema
    for it. Note: This JSON representation is compatible with the JSON Schema spec.
    This function is recursively called in order to generate the definitions for nested schemata
    :param information_schema: The data_type and column_name representations that come from querying
    the information_schema table
    :param json_schema: The starting json schema that gets passed in when creating array types
    :param input_node: defaults to properites, but can sometimes be written directly into the existing schema object
    :return: A JSON representation of the information schema that is compatible with the JSON Schema format.
    """
    if json_schema is None:
        json_schema = {"type": "object", "properties": {}}
    for column in information_schema:
        column["data_type"] = column["data_type"].lower()
        if column["data_type"].startswith("map"):
            column_type = (
                column["data_type"]
                .removeprefix("map")
                .removeprefix("(")
                .removesuffix(")")
            )
            map_key_value_types = lariat_string_utils.tokenize(column_type, ",")
            if len(map_key_value_types) == 1:
                map_value_type = column_type.strip().removeprefix("(").removesuffix(")")
            else:
                map_value_type = (
                    map_key_value_types[1].strip().removeprefix("(").removesuffix(")")
                )
            if "column_name" in column:
                json_schema[input_node][
                    column["column_name"]
                ] = transform_snowflake_to_json_schema(
                    information_schema=get_schema_from_type(map_value_type),
                    json_schema={"type": "object", "additionalProperties": {}},
                    input_node="additionalProperties",
                )
            else:
                json_schema[input_node] = transform_snowflake_to_json_schema(
                    information_schema=get_schema_from_type(map_value_type),
                    json_schema={"type": "object", "additionalProperties": {}},
                    input_node="additionalProperties",
                )
        elif column["data_type"].startswith("array"):
            column_type = (
                column["data_type"]
                .removeprefix("array")
                .removeprefix("(")
                .removesuffix(")")
            )
            if "column_name" in column:
                json_schema[input_node][column["column_name"]] = {}
                json_schema[input_node][column["column_name"]]["type"] = "array"
                json_schema[input_node][column["column_name"]]["items"] = {}
                json_schema[input_node][
                    column["column_name"]
                ] = transform_snowflake_to_json_schema(
                    information_schema=get_schema_from_type(column_type),
                    json_schema=json_schema[input_node][column["column_name"]],
                    input_node="items",
                )
            else:
                json_schema[input_node]["type"] = "array"
                json_schema[input_node]["items"] = {}
                json_schema[input_node] = transform_snowflake_to_json_schema(
                    information_schema=get_schema_from_type(column_type),
                    json_schema=json_schema[input_node],
                    input_node="items",
                )
        else:
            """
            This represents the recursive base case of just being a representation of a simple type.
            """
            output_record = {"type": SQL_TYPE_TO_JSON_TYPE[column["data_type"]][0]}
            if not SQL_TYPE_TO_JSON_TYPE[column["data_type"]][1] is None:
                output_record["format"] = SQL_TYPE_TO_JSON_TYPE[column["data_type"]][1]

            if "column_name" in column:
                json_schema[input_node][column["column_name"]] = output_record
            else:
                json_schema[input_node] = output_record
    return json_schema
