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
            "expect": [
                "1_29a086cdf9306bf1b2536fe7c9ba24fe37706d7c__86400_9cf7c8460aebd2a925bd4e4d9591760c26414fe9",
                "1_56daf1379b637b280f59b2d6f6af57200473f766__3600_9cf7c8460aebd2a925bd4e4d9591760c26414fe9",
                "1_56daf1379b637b280f59b2d6f6af57200473f766__86400_9cf7c8460aebd2a925bd4e4d9591760c26414fe9",
                "1__418b03f8dda432c1213c9b0568c4980be0200bd5_3600_9cf7c8460aebd2a925bd4e4d9591760c26414fe9",
                "1___172800_178ddfbecf63fb837393fbbee638ca622dd389c3",
                "1___3600_9cf7c8460aebd2a925bd4e4d9591760c26414fe9",
                "1___86400_9cf7c8460aebd2a925bd4e4d9591760c26414fe9",
                "1__c95b1c4bf964d05bbe7d517f4814f9abe2f9b259_3600_9cf7c8460aebd2a925bd4e4d9591760c26414fe9",
            ],
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
