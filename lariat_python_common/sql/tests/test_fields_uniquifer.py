import pytest

from lariat_python_common.test.utils import data_loader, get_test_labels
from lariat_python_common.sql.tests.data.fields_uniquifier.where_clause_uniquifier_tests import (
    tests,
)
from lariat_python_common.sql.fields_uniquifier import (
    DelimiterSeparatedListUniquifier,
    SQLWhereClauseUniquifier,
)


@pytest.mark.parametrize(
    "where_clause_variations, expected",
    data_loader(
        tests["where_clause_uniquifer"],
        ["where_clause_variations", "expected"],
    ),
    ids=get_test_labels(tests["where_clause_uniquifer"]),
)
def test_where_clause_uniquifer(where_clause_variations, expected):
    uniquifier = SQLWhereClauseUniquifier()
    for where_clause in where_clause_variations:
        assert uniquifier.uniquify_string(where_clause) == expected


@pytest.mark.parametrize(
    "comma_separated_list, expected",
    [
        ("B, A", "a1f86715f2bfd6bb0d269777e85422d81d222068"),
        ("A, B", "a1f86715f2bfd6bb0d269777e85422d81d222068"),
        ("A, B,    C, A", "686ba37964a11c4d915667a90ca6648ebff574fc"),
        ("A, A, B, C", "686ba37964a11c4d915667a90ca6648ebff574fc"),
        (
            "abc, whaterrrere,     ddd,    qq",
            "60cea2c8ac6db68c05f2a9ac2e799d23917aa79c",
        ),
        ("abc,ddd,whaterrrere,qq", "60cea2c8ac6db68c05f2a9ac2e799d23917aa79c"),
        ("", ""),
    ],
    ids=[
        "Easy String",
        "Easy String",
        "Medium String",
        "Medium String",
        "Hard String",
        "Hard String",
        "Empty String",
    ],
)
def test_delimiter_sperated_list_uniquifer(comma_separated_list, expected):
    uniquifier = DelimiterSeparatedListUniquifier()
    assert uniquifier.uniquify_string(comma_separated_list) == expected
