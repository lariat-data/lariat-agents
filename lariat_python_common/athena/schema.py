"""
    All code related to interpreting and transforming Athena Schemata
"""
from typing import Any, Dict, List, Optional
import lariat_python_common.string.utils as lariat_string_utils

"""
    Canonical Mapping of Athena types to JSON types and formats
"""
SQL_TYPE_TO_JSON_TYPE = {
    "varchar": ("string", None),
    "date": ("string", "date"),
    "time": ("string", "time"),
    "double": ("number", "double"),
    "float": ("number", "float"),
    "real": ("number", "float"),
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

PARQUET_FORMAT_STRING = "parquet"

"""
    Canonical Mapping of JSON types and formats to Athena types
"""
JSON_TYPE_TO_SQL_TYPE = {
    # string
    ("string", None): "string",
    ("string", "date-time"): "timestamp",
    ("string", "date-time-string"): "string",
    ("string", "date"): "date",
    ("string", "date-string"): "string",
    ("string", "time"): "time",
    ("string", "time-string"): "string",
    ("string", "decimal"): "decimal",
    # number
    ("number", None): "double",
    ("number", "double"): "double",
    ("number", "float"): "float",
    ("number", "decimal"): "decimal",
    # integer
    ("integer", None): "bigint",
    ("integer", ""): "bigint",
    ("integer", "int"): "bigint",
    ("integer", "bigint"): "bigint",
    ("integer", "date-time"): "timestamp",
    ("integer", "date"): "date",
    ("integer", "time"): "time",
    # boolean
    ("boolean", None): "boolean",
    # array
    ("array", None): "array",
    # object
    ("object", None): "struct",
}

ROOT_IDENTIFIER_FIELD = "$schema"
PROPERTIES_FIELD = "properties"
ADDITIONAL_PROPERTIES_FIELD = "additionalProperties"
TYPE_FIELD = "type"
REQUIRED_FIELD = "required"
ITEMS_FIELD = "items"
FORMAT_FIELD = "format"
SCALE_FIELD = "scale"
PRECISION_FIELD = "precision"


def get_schema_retrieval_query(table_names, table_schema):
    """
    Given list of table names and table schema, it returns the needed query to run against athena to
    get these table info, it also support the '*' wild card
    example
    table_names = ['v', 'v*'], table_schema=xxx
    result:
    "SELECT table_schema,table_name,column_name,data_type FROM information_schema.columns where table_schema=xxx and
    table_name IN ('v') amd table_name like 'v%'"
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
        "SELECT table_schema,table_name,column_name,data_type,extra_info FROM"
        f" information_schema.columns where table_schema='{table_schema}'"
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


def transform_athena_to_json_schema(
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
        if column["data_type"].startswith("row"):
            column_type = (
                column["data_type"]
                .removeprefix("row")
                .removeprefix("(")
                .removesuffix(")")
            )
            if "column_name" in column:
                json_schema[input_node][
                    column["column_name"]
                ] = transform_athena_to_json_schema(
                    information_schema=get_schema_from_type(column_type)
                )
            else:
                json_schema[input_node] = transform_athena_to_json_schema(
                    information_schema=get_schema_from_type(column_type)
                )
        elif column["data_type"].startswith("map"):
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
                ] = transform_athena_to_json_schema(
                    information_schema=get_schema_from_type(map_value_type),
                    json_schema={"type": "object", "additionalProperties": {}},
                    input_node="additionalProperties",
                )
            else:
                json_schema[input_node] = transform_athena_to_json_schema(
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
                ] = transform_athena_to_json_schema(
                    information_schema=get_schema_from_type(column_type),
                    json_schema=json_schema[input_node][column["column_name"]],
                    input_node="items",
                )
            else:
                json_schema[input_node]["type"] = "array"
                json_schema[input_node]["items"] = {}
                json_schema[input_node] = transform_athena_to_json_schema(
                    information_schema=get_schema_from_type(column_type),
                    json_schema=json_schema[input_node],
                    input_node="items",
                )
        else:
            """
            This represents the recursive base case of just being a representation of a simple type.
            """
            if column["data_type"].startswith("varchar"):
                column["data_type"] = "varchar"
            elif column["data_type"].startswith("timestamp("):
                column["data_type"] = "timestamp"
            output_record = {"type": SQL_TYPE_TO_JSON_TYPE[column["data_type"]][0]}
            if not SQL_TYPE_TO_JSON_TYPE[column["data_type"]][1] is None:
                output_record["format"] = SQL_TYPE_TO_JSON_TYPE[column["data_type"]][1]

            if "column_name" in column:
                json_schema[input_node][column["column_name"]] = output_record
            else:
                json_schema[input_node] = output_record
    return json_schema


def generate_create_table(
    table: str,
    database: str,
    source_path: str,
    jsonschema: Dict[str, Any],
    partition_columns: Optional[List] = None,
    format: str = PARQUET_FORMAT_STRING,
) -> str:
    """
    Gets a create table ddl for an athena table given a schema
    Currently only supports parquet format
    """
    if format != PARQUET_FORMAT_STRING:
        raise ValueError(
            f"Unsupported format for create table. Does not support {format} format"
        )

    create_table_sql = """CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table}` (
    {columns}
    )
    """
    if partition_columns:
        partition_predicate = (
            "PARTITIONED BY("
            + ",\n".join([f"`{p}` string" for p in partition_columns])
            + "\n)\n"
        )
        create_table_sql = create_table_sql + partition_predicate

    create_table_sql = (
        create_table_sql
        + """
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES ('serialization.format' = '1')
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION '{source_path}'
    """
    )

    format_map = {
        "db": database,
        "table": table,
        "columns": get_columns(None, jsonschema),
        "source_path": source_path,
    }

    create_table_sql = create_table_sql.format(**format_map)
    return create_table_sql


def get_columns(
    field: Optional[str], jsonschema: Dict[str, Any], in_struct: bool = False
) -> str:
    struct_delim = ":" if in_struct else ""
    # if we are at the root of schema (ie. $schema exists) we need to start from its properties
    if jsonschema.get(ROOT_IDENTIFIER_FIELD) is not None:
        return get_columns(field, jsonschema.get(PROPERTIES_FIELD), in_struct)

    # if properties are available, we need to recurse again
    if jsonschema.get(PROPERTIES_FIELD) is not None:
        sql = get_columns(None, jsonschema.get(PROPERTIES_FIELD), True)
        # for nested fields we normalize the sql by removing newlines
        sql = " ".join(sql.split())

        # if we came here from within an array, field name is not needed
        if field:
            return "`{field}`{struct_delim} struct<{inner_columns}>".format(
                field=field, inner_columns=sql, struct_delim=struct_delim
            )
        else:
            return "struct<{inner_columns}>".format(inner_columns=sql)

    if jsonschema.get(ADDITIONAL_PROPERTIES_FIELD) is not None:
        sql = get_columns(None, jsonschema.get(ADDITIONAL_PROPERTIES_FIELD), True)
        # for nested fields we normalize the sql by removing newlines
        sql = " ".join(sql.split())

        # if we came here from within an array, field name is not needed
        if field:
            return "`{field}`{struct_delim} map<string, {inner_columns}>".format(
                field=field, inner_columns=sql, struct_delim=struct_delim
            )
        else:
            return "map<string, {inner_columns}>".format(inner_columns=sql)

    # if items are available, we are within an array and need to recurse again
    if jsonschema.get(ITEMS_FIELD) is not None:
        # pass None as field name to prevent it from appearing in inner column sql
        sql = get_columns(None, jsonschema.get(ITEMS_FIELD, in_struct))
        # for nested fields we normalize the sql by removing newlines
        sql = " ".join(sql.split())
        return "`{field}`{struct_delim} array<{inner_columns}>".format(
            field=field, inner_columns=sql, struct_delim=struct_delim
        )

    # if we are within an object (ie. type doesn't exist), we need to iterate over all fields
    if jsonschema.get(TYPE_FIELD) is None:
        first = True
        sql = ""
        for field in jsonschema:
            if not first:
                sql += ",\n    "
            sql += get_columns(field, jsonschema[field], in_struct)
            first = False

        return sql

    # if we are within a field, we can create a column
    key = (jsonschema[TYPE_FIELD], jsonschema.get(FORMAT_FIELD, None))
    sql_type = JSON_TYPE_TO_SQL_TYPE[key]

    # if decimal, we need the type arguments too
    if jsonschema.get(FORMAT_FIELD) == "decimal":
        sql_type += "({precision}, {scale})".format(
            precision=jsonschema.get(PRECISION_FIELD), scale=jsonschema.get(SCALE_FIELD)
        )

    # if we came here from within an array, field name is not needed
    if field:
        return "`{field}`{struct_delim} {sql_type}".format(
            field=field, sql_type=sql_type, struct_delim=struct_delim
        )
    else:
        return "{sql_type}".format(sql_type=sql_type)
