from lariat_python_common.test.utils import TestComplexity, TEST_COMPLEXITY_KEY
import sqlparse

tests = {
    "get_hashed_fields_tests": {
        "base_case": {
            "test_group_field": "",
            "test_filter_str": "",
            "test_evaluation_interval": "",
            "test_lookback_window": "",
            "test_computed_dataset_id": "",
            "test_seperator": "_",
            "expect": "____",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_0": {
            "test_group_field": "test1,test2",
            "test_filter_str": "",
            "test_evaluation_interval": "*/5 * * * *",
            "test_lookback_window": "3600",
            "test_computed_dataset_id": "2",
            "test_seperator": "_",
            "expect": "2_64f3a05b7a514108144be81b4e01df6dc1bd465b__3600_dd41b12a27b0065a5ae6237c90e050eeded59848",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_1": {
            "test_group_field": "test1,test2",
            "test_filter_str": "",
            "test_evaluation_interval": "*/5 * * * *",
            "test_lookback_window": "3600",
            "test_computed_dataset_id": "2",
            "test_seperator": "|",
            "expect": "2|64f3a05b7a514108144be81b4e01df6dc1bd465b||3600|dd41b12a27b0065a5ae6237c90e050eeded59848",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_2": {
            "test_group_field": "test1,test2",
            "test_filter_str": "(tri = 3 OR quad Like '%quad%')",
            "test_evaluation_interval": "*/5 * * * *",
            "test_lookback_window": "3600",
            "test_computed_dataset_id": "2",
            "test_seperator": "_",
            "expect": "2_64f3a05b7a514108144be81b4e01df6dc1bd465b_f982ecb053b047f754c779a6e7405851f5db85ed_"
            "3600_dd41b12a27b0065a5ae6237c90e050eeded59848",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
    },
    "match_count_distinct_get_operand_tests": {
        "base_case": {
            "test_statement": sqlparse.parse("SUM(device_id)")[0],
            "expect": (False, None),
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_0": {
            "test_statement": sqlparse.parse("COUNT(device_id)")[0],
            "expect": (False, None),
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_1": {
            "test_statement": sqlparse.parse("COUNT(DISTINCT device_id)")[0],
            "expect": (True, "device_id"),
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
    },
    "match_decile_get_operand_tests": {
        "base_case": {
            "test_statement": sqlparse.parse("SUM(device_id)")[0],
            "expect": (False, None, None),
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_0": {
            "test_statement": sqlparse.parse("COUNT(device_id)")[0],
            "expect": (False, None, None),
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_1": {
            "test_statement": sqlparse.parse("APPROX_percentile(device_id, 0.5)")[0],
            "expect": (True, "device_id", "0.5"),
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
    },
}
