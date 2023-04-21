from lariat_python_common.test.utils import TestComplexity, TEST_COMPLEXITY_KEY

tests = {
    "where_clause_uniquifer": {
        "empty_filter_0": {
            "where_clause_variations": [""],
            "expected": "",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "binary_operator_0": {
            "where_clause_variations": ["column2 AND column1", "column1 AND column2"],
            "expected": "e6025f626f422c4f15340390d2793b8fba4ee27b",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "binary_operator_1": {
            "where_clause_variations": ["B = 2 AND A = 1", "A = 1 AND B = 2"],
            "expected": "7bee5c8f705c80a9555f826345aab165d25c7fb1",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "binary_operator_2": {
            "where_clause_variations": [
                "B = 1 AND A = 1 AND C = 1",
                "A = 1 AND B = 1 AND C = 1",
                "C = 1 AND A = 1 AND B = 1",
            ],
            "expected": "7b7b4da0178de6fdda836c369ea770c8ed2b901f",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "binary_operator_3": {
            "where_clause_variations": [
                "(X = 12 OR WQWE = 'qwe') AND Y > 12",
                "(WQWE = 'qwe' OR X = 12) AND Y > 12",
                "Y > 12 AND (X = 12 OR WQWE = 'qwe')",
            ],
            "expected": "b851cd4b045d1f3ed51eb61098200e7c8d06a876",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "binary_operator_4": {
            "where_clause_variations": [
                "Y  = 1 AND X like 'abc'",
                "X like 'abc' AND Y  = 1",
            ],
            "expected": "a31e1cc51f60cdfd4c333c8eff0bddc86e25a5d8",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "binary_operator_5": {
            "where_clause_variations": [
                "B IS NULL AND C > 100 AND F > 10",
                "F > 10 AND C > 100 AND B IS NULL",
                "C > 100 AND F > 10 AND B IS NULL",
            ],
            "expected": "c48622bf3aa2ff168b5141054de7067265138038",
            TEST_COMPLEXITY_KEY: TestComplexity.SIMPLE.name,
        },
        "redundant_parenthesis": {
            "where_clause_variations": [
                "(F = 1 AND (C > 100 OR (X > 10)))",
                "(F = 1 AND ((X > 10) OR C > 100))",
                "((C > 100 OR (X > 10)) AND F = 1)",
            ],
            "expected": "f9f3cbec623ef2eb7a4bcb3b18e7c202f4d14c70",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "not_keyword_0": {
            "where_clause_variations": [
                "y = 12 AND NOT x = 12",
                "NOT x = 12 AND y = 12",
            ],
            "expected": "96ba329f1037a454a8658a5b08c04834ccab8ce1",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "not_keyword_1": {
            "where_clause_variations": [
                "Z > 2 AND NOT X IS NULL",
                "NOT X IS NULL AND Z > 2",
            ],
            "expected": "6b70d4924b2ecc666830016e4049047ee27bd305",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "not_keyword_2": {
            "where_clause_variations": [
                "Z <= 12 AND NOT X BETWEEN 12 AND 13",
                "NOT X BETWEEN 12 AND 13 AND Z <= 12",
            ],
            "expected": "061ca8e5c687452cf9567f09e0cc348bd8c1395a",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "not_keyword_3": {
            "where_clause_variations": [
                "NOT (Z <= 12) AND X = 2",
                "X = 2 AND NOT (Z <= 12)",
            ],
            "expected": "9e5ff3a7367f4c59d4f7feff4256eabb3d557cc8",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "not_keyword_4": {
            "where_clause_variations": [
                "NOT (C > 1 AND B < 2) AND (A = 1)",
            ],
            "expected": "be983107d036fb2e2e4a9d379651666e7765349c",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "between_keyword_0": {
            "where_clause_variations": [
                "Z = 1 AND X BETWEEN 12 AND 13",
                "X BETWEEN 12 AND 13 AND Z = 1",
            ],
            "expected": "73a1890cd9349dca9e445e850b0a0af65b51fd19",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "between_keyword_1": {
            "where_clause_variations": [
                "Z = 1 AND X BETWEEN 12 AND 13",
                "X BETWEEN 12 AND 13 AND Z = 1",
            ],
            "expected": "73a1890cd9349dca9e445e850b0a0af65b51fd19",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "not_between_keyword": {
            "where_clause_variations": [
                "A = 1 OR X NOT BETWEEN 12 AND 13",
                "X NOT BETWEEN 12 AND 13 OR A = 1",
            ],
            "expected": "dc9cc3bf9b20a4fa8b4de49878fd5fc6581632b0",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "is_not_null_keyword": {
            "where_clause_variations": [
                "Y IS NULL AND X IS NOT NULL",
                "X IS NOT NULL AND Y IS NULL",
            ],
            "expected": "cbe34ad898d5cf85bbd971641502b51c2bb91d23",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "in_keyword": {
            "where_clause_variations": [
                "Y IN (1,2,3,4) AND A = 1",
                "A = 1 AND Y IN (1,2,3,4)",
            ],
            "expected": "18219e2e38d41178e0907f086cbdf6e4bb401831",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "not_in_keyword": {
            "where_clause_variations": [
                "Y NOT IN (1,2,3,4) AND A = 1",
                "A = 1 AND Y NOT IN (1,2,3,4)",
            ],
            "expected": "7ca5bc3d3e7db8392bdf5ed32d0a7ac10c3dd1dd",
            TEST_COMPLEXITY_KEY: TestComplexity.MEDIUM.name,
        },
        "complex_query_1": {
            "where_clause_variations": [
                """(deviceid = "thermopak" AND company_id = "thermopak") OR (company_id = "black_and_decker" AND deviceid = "drill")""",
                """(deviceid = "thermopak" AND company_id = "thermopak") OR (deviceid = "drill" AND company_id = "black_and_decker")""",
                """(company_id = "thermopak" AND deviceid = "thermopak") OR (deviceid = "drill" AND company_id = "black_and_decker")""",
                """(deviceid = "drill" AND company_id = "black_and_decker") OR (company_id = "thermopak" AND deviceid = "thermopak")""",
            ],
            "expected": "393c8d1dfe5d46e2d77e0a216f4d0adc8796f948",
            TEST_COMPLEXITY_KEY: TestComplexity.COMPLEX.name,
        },
        "complex_query_2": {
            "where_clause_variations": [
                """(X = 1 AND Y NOT BETWEEN 12 AND 13) OR Z IS NOT NULL""",
                """(Y NOT BETWEEN 12 AND 13 AND X = 1) OR Z IS NOT NULL""",
                """Z IS NOT NULL OR (X = 1 AND Y NOT BETWEEN 12 AND 13)""",
            ],
            "expected": "f7a565e86b02288146ae4ad5e39bda41a425d1e0",
            TEST_COMPLEXITY_KEY: TestComplexity.COMPLEX.name,
        },
        "complex_query_3": {
            "where_clause_variations": [
                """(B IS NOT NULL AND C <> 13 AND A = 1) OR (C NOT like '%a%' AND A NOT IN (1,2,3)) OR Z BETWEEN 12 AND 15 OR A = 33""",
                """A = 33 OR (B IS NOT NULL AND C <> 13 AND A = 1) OR (C NOT like '%a%' AND A NOT IN (1,2,3)) OR Z BETWEEN 12 AND 15""",
                """(C NOT like '%a%' AND A NOT IN (1,2,3)) OR (B IS NOT NULL AND C <> 13 AND A = 1) OR Z BETWEEN 12 AND 15 OR A = 33""",
                """(C NOT like '%a%' AND A NOT IN (1,2,3)) OR Z BETWEEN 12 AND 15 OR (B IS NOT NULL AND C <> 13 AND A = 1) OR A = 33""",
            ],
            "expected": "2ea3f041be3a26f771fc88e19573ea4ee9824cfe",
            TEST_COMPLEXITY_KEY: TestComplexity.COMPLEX.name,
        },
        "complex_query_4": {
            "where_clause_variations": [
                """(b_and_something ilike '%qwe%' AND a_between BETWEEN 13 AND 15) OR (column1 IS NOT NULL AND column2 NOT IN (1,2,3))""",
                """(a_between BETWEEN 13 AND 15 AND b_and_something ilike '%qwe%') OR (column2 NOT IN (1,2,3) AND column1 IS NOT NULL)""",
                """(column1 IS NOT NULL AND column2 NOT IN (1,2,3)) OR (b_and_something ilike '%qwe%' AND a_between BETWEEN 13 AND 15)""",
            ],
            "expected": "43704ac96b4d7e4be975cbac1f3f6ced5f2ffc66",
            TEST_COMPLEXITY_KEY: TestComplexity.COMPLEX.name,
        },
    },
}
