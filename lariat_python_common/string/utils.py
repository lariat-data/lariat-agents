"""
    Lariat Python Utilities for String Manipulation/Handling
"""


def tokenize(string, separator=",", open_delimiter="(", close_delimiter=")"):
    """
    Tokenize a string on a separator, but do so only when not within the open & close delimiters.
    Examples:
        INPUT: statement varchar, testing row(something array(varchar))
        OUTPUT: ['statement varchar','testing row(something array(varchar)']

        INPUT: testing row(something array(varchar))
        SEPERATOR: " "
        OUTPUT: ['testing','row(something array(varchar))']
    :param string: string to tokenize
    :param separator: seperator to tokenize based off
    :param open_delimiter: open delimiter to no longer tokenize based off
    :param close_delimiter: the closed delimiter from which tokenization can begin again if an open delimiter was
    encountered before
    :return: correctly tokenized list
    """
    separated_list = []
    stack = []
    chunk = ""
    in_delimiter = False
    for character in string:
        if character == separator and not in_delimiter:
            separated_list.append(chunk)
            chunk = ""
        else:
            chunk += character
            if character == open_delimiter:
                stack.append(open_delimiter)
                if not in_delimiter:
                    in_delimiter = True
            if character == close_delimiter and in_delimiter:
                stack.pop()
                if not stack:
                    in_delimiter = False
    separated_list.append(chunk)
    return separated_list
