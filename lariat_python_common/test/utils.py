from enum import Enum

TEST_COMPLEXITY_KEY = "complexity"


class TestComplexity(Enum):
    __test__ = False

    SIMPLE = "SIMPLE"
    MEDIUM = "MEDIUM"
    COMPLEX = "COMPLEX"


def data_loader(test_data, test_param_names):
    """ """
    final_test_data = []
    for _, test_data_values in test_data.items():
        test_data_values_tuple = tuple(
            [test_data_values[parameter_name] for parameter_name in test_param_names]
        )
        final_test_data.append(test_data_values_tuple)

    return final_test_data


def get_test_labels(test_data):
    return [
        str(test_data[TEST_COMPLEXITY_KEY]) + ": " + test_name
        for test_name, test_data in test_data.items()
    ]
