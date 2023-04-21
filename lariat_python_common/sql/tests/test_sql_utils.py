import pytest
import lariat_python_common.sql.utils as lariat_sql_utils
from lariat_python_common.test.utils import data_loader, get_test_labels
from lariat_python_common.sql.tests.data.sql_utils.test_cases import tests


@pytest.mark.parametrize(
    "test_group_field, test_filter_str, test_evaluation_interval, test_lookback_window,test_computed_dataset_id, "
    "test_seperator, expect",
    data_loader(
        tests["get_hashed_fields_tests"],
        [
            "test_group_field",
            "test_filter_str",
            "test_evaluation_interval",
            "test_lookback_window",
            "test_computed_dataset_id",
            "test_seperator",
            "expect",
        ],
    ),
    ids=get_test_labels(tests["get_hashed_fields_tests"]),
)
def test_get_hashed_fields(
    test_group_field,
    test_filter_str,
    test_evaluation_interval,
    test_lookback_window,
    test_computed_dataset_id,
    test_seperator,
    expect,
):
    hashed_fields = lariat_sql_utils.get_hashed_fields(
        group_fields=test_group_field,
        filter_str=test_filter_str,
        evaluation_interval=test_evaluation_interval,
        lookback_window=test_lookback_window,
        computed_dataset_id=test_computed_dataset_id,
        seperator=test_seperator,
    )
    assert hashed_fields == expect


@pytest.mark.parametrize(
    "test_statement, expect",
    data_loader(
        tests["match_count_distinct_get_operand_tests"],
        [
            "test_statement",
            "expect",
        ],
    ),
    ids=get_test_labels(tests["match_count_distinct_get_operand_tests"]),
)
def test_match_count_distinct_get_operand(test_statement, expect):
    actual = lariat_sql_utils.match_count_distinct_get_operand(test_statement)
    assert actual == expect


@pytest.mark.parametrize(
    "test_statement, expect",
    data_loader(
        tests["match_decile_get_operand_tests"],
        [
            "test_statement",
            "expect",
        ],
    ),
    ids=get_test_labels(tests["match_decile_get_operand_tests"]),
)
def test_match_decile_get_operand(test_statement, expect):
    actual = lariat_sql_utils.match_decile_get_operand(test_statement)
    assert actual == expect
