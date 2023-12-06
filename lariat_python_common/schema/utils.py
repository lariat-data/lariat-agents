"""
    Lariat Python Utilities for Handling Json
"""
import json


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
