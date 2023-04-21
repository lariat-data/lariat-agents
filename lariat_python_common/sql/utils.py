import logging

import lariat_python_common.sql.fields_uniquifier as fields_uniquifier
import sqlparse
from sqlalchemy import create_engine
import os


SAFE_CAST_TO_DOUBLE_SINGLE_ARG_FUNCTIONS = ["stddev"]


def get_hashed_fields(
    group_fields: str,
    filter_str: str,
    evaluation_interval: str,
    lookback_window: int,
    computed_dataset_id: int,
    seperator: str = "_",
    sketch_type: str = None,
):
    """
    Provide a unique hash identifier given the arguments that make up indicators for a given computed dataset, and
    filter,group,evaluation interval and lookback_window.

    This is used to identify compute families - i.e. indicator computations that can be grouped together as part
    of the same query.

    :param group_fields: comma separated list of group fields e.g.: "field_1, field_2"
    :param filter_str: sql filter (i.e the part that comes after the where) or empty string
                       e.g: (col_1 = 2 AND col_3 like '%example%';)
    :param evaluation_interval: cron string representing frequency of evaluation
    :param lookback_window: integer representing how far back to look to calculate the indicator
    :param computed_dataset_id: integer representing the computed dataset id
    :param seperator: defaults to underscore, how to separate the individual hashes
    :param sketch_type: Returns the kind of sketch this is NONE, DECILE, COUNT_DISTINCT
    :return: a combined string of each field individually hashed and seperated as defined by the seperator argument
    """
    hashed_group_fields = (
        fields_uniquifier.DelimiterSeparatedListUniquifier().uniquify_string(
            group_fields
        )
    )
    hashed_filter = fields_uniquifier.SQLWhereClauseUniquifier().uniquify_string(
        filter_str
    )

    hashed_eval_interval = (
        fields_uniquifier.DelimiterSeparatedListUniquifier().uniquify_string(
            evaluation_interval
        )
    )
    if not sketch_type:
        return seperator.join(
            [
                str(computed_dataset_id),
                hashed_group_fields,
                hashed_filter,
                str(lookback_window),
                hashed_eval_interval,
            ]
        )
    else:
        return seperator.join(
            [
                str(computed_dataset_id),
                str(sketch_type),
                hashed_group_fields,
                hashed_filter,
                str(lookback_window),
                hashed_eval_interval,
            ]
        )


def match_count_distinct_get_operand(statement: sqlparse.sql.Statement):
    """
    Given a parsed sql statement from sqlparse return true if the statement is a count distinct calculation and false if
    not. Also return the operand upon which Count Distinct is being performed on.
    :param statement: sqlparse.sql.Statement - generally of a calculation in an indicator
    :return: Tuple (boolean, str) of (is_count_distinct, operand) -> where operand is None if is_count_distinct is False
    """
    if type(statement.tokens[0]) == sqlparse.sql.Function:
        if statement.tokens[0][0].value.lower() == "count":
            for i in statement.tokens[0].flatten():
                if str(i.ttype) == "Token.Keyword" or str(i.ttype) == "Token.Name":
                    if i.value.lower() == "distinct":
                        operand = statement.tokens[0].get_parameters()[0].value
                        return True, operand
    return False, None


def match_decile_get_operand(statement: sqlparse.sql.Statement):
    """
    Given a parsed sql statement from sqlparse return true if the statement is a percentile calculation and false if
    not. Also return the operand upon which Percentile Calculation is being performed on and the decile of interest
    (i.e 0.1 if the operation is to get the 10th percentile value)
    :param statement: sqlparse.sql.Statement - generally of a calculation in an indicator
    :return: Tuple (boolean, str, str) of (is_percentile, operand, decile_value)
        -> where operand is None and decile_value is None  if is_percentile is False
    """
    operand = None
    decile_value = None
    if type(statement.tokens[0]) == sqlparse.sql.Function:
        if statement.tokens[0][0].value.lower() == "approx_percentile":
            for i in statement.tokens[0][1].flatten():
                if str(i.ttype) == "Token.Name" and not operand:
                    operand = i.value
                elif str(i.ttype).startswith("Token.Literal.Number"):
                    decile_value = i.value
            return True, operand, decile_value
    return False, None, None


def safe_cast_calculation(statement: sqlparse.sql.Statement):
    fully_cast_calculation = str(statement)
    try:
        if type(statement.tokens[0]) == sqlparse.sql.Function:
            function_type = statement.tokens[0][0].value
            if function_type.lower() in SAFE_CAST_TO_DOUBLE_SINGLE_ARG_FUNCTIONS:
                cast_arguments = f"CAST({statement.tokens[0][1].value.removeprefix('(').removesuffix(')')} as double)"
                fully_cast_calculation = f"{function_type}({cast_arguments})"
    except Exception as e:
        logging.warning(
            f"Safe casting calculation failed for query {fully_cast_calculation} {e}"
        )
    return fully_cast_calculation


def get_default_db_conn():
    return create_engine(
        "postgresql+psycopg2://"
        + os.environ.get("DATABASE_USER", "postgres")
        + ":"
        + os.environ.get("DATABASE_USER_PASSWORD", "")
        + "@"
        + os.environ.get(
            "DATABASE_HOST",
            "localhost",
        )
        + "/"
        + os.environ.get("LARIAT_DB_NAME", "lariat")
    )
