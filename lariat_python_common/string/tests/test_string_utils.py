import pytest
import lariat_python_common.string.utils as lariat_string_utils


@pytest.mark.parametrize(
    "tokenize_string,seperator,open_delimiter,closed_delimiter,expected",
    [
        ("B,A,C", ",", "(", ")", ["B", "A", "C"]),
        ("B(A,C)", ",", "(", ")", ["B(A,C)"]),
        ("B(A,C),X,Y,Z(,345", ",", "(", ")", ["B(A,C)", "X", "Y", "Z(,345"]),
        (
            "testing row(something array(varchar))",
            " ",
            "(",
            ")",
            ["testing", "row(something array(varchar))"],
        ),
        (
            "statement varchar,testing row(something array(varchar)",
            ",",
            "(",
            ")",
            ["statement varchar", "testing row(something array(varchar)"],
        ),
    ],
    ids=[
        "Easy_String_0",
        "Easy_String_1",
        "Easy_String_2",
        "Easy_String_3",
        "Easy_String_4",
    ],
)
def test_tokenize(
    tokenize_string, seperator, open_delimiter, closed_delimiter, expected
):
    assert (
        lariat_string_utils.tokenize(
            tokenize_string, seperator, open_delimiter, closed_delimiter
        )
        == expected
    )
