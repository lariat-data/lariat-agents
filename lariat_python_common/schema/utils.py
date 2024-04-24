"""
    Lariat Python Utilities for Getting JSON Schemas from different sources
"""
import json
import pyarrow as pa
import pyarrow.parquet as pq
from genson import SchemaBuilder
import pandas as pd


def cast_value_by_inferred_type(value):
    try:
        float_val = float(value)
        if float_val.is_integer():
            return int(float_val)
        return float_val
    except ValueError:
        # Return the original value if it can't be converted to a numeric type.
        return value


def update_schema_with_json(existing_schema, additional_json):
    if additional_json:
        type_modified_additional_json = {
            k: cast_value_by_inferred_type(v) for k, v in additional_json.items()
        }
        builder = SchemaBuilder()
        builder.add_object(type_modified_additional_json)
        additional_properties_schema = builder.to_schema()
        existing_schema["properties"].update(additional_properties_schema["properties"])
    return existing_schema


def clean_schema(schema):
    """
    Recursively cleans a schema of unused fields
    """
    if isinstance(schema, dict):
        for key in list(schema.keys()):
            if key in ["required", "$schema"]:
                del schema[key]
            elif key == "type":
                val = schema[key]
                if val not in ["object", "array"]:
                    schema[key] = "string"
            else:
                clean_schema(schema[key])
    elif isinstance(schema, list):
        for item in schema:
            clean_schema(item)
    else:
        pass


def get_clean_schema(builder):
    """
    Given a schema builder, returns a clean string representation of the schema
    """
    schema = builder.to_schema()
    clean_schema(schema)
    return json.dumps(schema, sort_keys=True)


def process_arrow_schema_field(field):
    if isinstance(field.type, pa.StructType):
        properties = {}
        for subfield in field.type:
            properties[subfield.name] = process_arrow_schema_field(subfield)
        return {"type": "object", "properties": properties}
    elif isinstance(field.type, pa.ListType):
        return {
            "type": "array",
            "items": process_arrow_schema_field(field.type.value_field),
        }
    else:  # Map PyArrow data types to JSON Schema data types
        data_type = field.type.to_pandas_dtype()
        json_type = {
            int: "integer",
            float: "number",
            bool: "boolean",
            str: "string",
        }.get(data_type, "string")
        return {"type": json_type}


def convert_arrow_schema_to_json_schema(arrow_schema):
    """
    Given an arrow table schema - return a clean string representation of the JSON Schema
    :param arrow_schema:
    :return: string representation of json schema dict with sorted keys
    """
    json_schema = {"type": "object", "properties": {}}
    for field in arrow_schema:
        json_schema["properties"][field.name] = process_arrow_schema_field(field)
    clean_schema(json_schema)
    return json.dumps(json_schema, sort_keys=True)


def get_schema_from_parquet_object(file_content, additional_json=None):
    """
    Given a parquet file loaded in as bytes, get the json schema representation
    Additional JSON represents other fields to be added into the schema. It is a non-nested dict
    We use this when we have a few additional meta fields to add into a schema representation (e.g. partition vars)
    TODO: Replace with a version that doesn't require reading the entire file and looking directly at the header
    :param file_content:
    :param additional_json
    :return:
    """
    table = pq.read_table(pa.BufferReader(file_content))
    arrow_schema = table.schema
    existing_schema = json.loads(convert_arrow_schema_to_json_schema(arrow_schema))
    updated_schema = update_schema_with_json(existing_schema, additional_json)
    return updated_schema


def get_schema_from_json(json_content, additional_json=None):
    """
    Get the JSONSchema of a json record. Additional JSON represents other fields to be added into the schema.
    It is a non-nested dict
    We use this when we have a few additional meta fields to add into a schema representation (e.g. partition vars)
    :param json_content:
    :param additional_json:
    :return:
    """
    if additional_json:
        type_modified_additional_json = {
            k: cast_value_by_inferred_type(v) for k, v in additional_json.items()
        }
        json_content.update(type_modified_additional_json)
    builder = SchemaBuilder()
    builder.add_object(json_content)
    cleaned_schema = get_clean_schema(builder)
    return cleaned_schema


# Function to map pandas dtypes to JSON Schema data types
def dtype_to_jsonschema_type(dtype):
    if dtype.startswith("int"):
        return "integer"
    elif dtype.startswith("float"):
        return "number"
    elif dtype == "bool":
        return "boolean"
    else:
        return "string"


def get_schema_from_df(df):
    return {
        "type": "object",
        "properties": {
            column: {"type": dtype_to_jsonschema_type(str(df[column].dtype))}
            for column in df.columns
        },
    }


def get_schema_from_csv(csv_content, additional_json=None):
    """
    Get the JSONSchema of a json record. Additional JSON represents other fields to be added into the schema.
    It is a non-nested dict
    We use this when we have a few additional meta fields to add into a schema representation (e.g. partition vars)
    :param json_content:
    :param additional_json:
    :return:
    """
    df = pd.read_csv(pd.io.common.StringIO(csv_content), nrows=5)
    existing_schema = get_schema_from_df(df)
    updated_schema = update_schema_with_json(existing_schema, additional_json)
    return updated_schema
