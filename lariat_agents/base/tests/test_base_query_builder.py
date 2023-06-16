from lariat_agents.base.base_query_builder import BaseQueryBuilder


def test_construct_select_predicate():
    """
    This test tests the construct_select_predicate functionality in BaseQueryBuilder.
    """
    response = BaseQueryBuilder.construct_select_predicate({"col1": "alias"})
    assert response == "col1 as alias"
    response = BaseQueryBuilder.construct_select_predicate("col2")
    assert response == "col2 as col2"


def test_add_timestamp_fields():
    """
    This test tests the add_timestamp_fields functionality in BaseQueryBuilder.
    """
    response = BaseQueryBuilder.add_timestamp_fields(
        timestamp_col="timestamp", evaluation_time=1654646400, lookback_time=1000
    )
    assert (
        response
        == "MIN(timestamp) as _result_min_ts, MAX(timestamp) as _result_max_ts, "
        "1654646400 as _lookback_range_end_ts, 1654645400 as _lookback_range_start_ts"
    )
