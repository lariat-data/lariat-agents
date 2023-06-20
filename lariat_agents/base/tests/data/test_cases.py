import os

import pandas as pd
from lariat_python_common.test.utils import TEST_COMPLEXITY_KEY, TestComplexity

DATA_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
INDICATORS_DATASET_PATH = os.path.join(DATA_DIR_PATH, "test_indicators_dataset.json")

tests = {
    "execute_indicators": {
        "base_case": {
            "indicators": pd.DataFrame(),
            "expect": True,
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_0": {
            "indicators": pd.read_json(INDICATORS_DATASET_PATH),
            "expect": None,
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
    },
    "get_sketch_type_from_calculation": {
        "base_case": {
            "calculation": "100.0 * COUNT(test)/COUNT(*)",
            "expect": "NONE",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_0": {
            "calculation": "COUNT(distinct partnerid)",
            "expect": "COUNT_DISTINCT",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "simple_case_1": {
            "calculation": "approx_percentile(price, 0.5)",
            "expect": "DECILE",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
    },
}
