"""
    Lariat Python Utilities for String Manipulation/Handling
"""
import re


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


def match_lariat_file_partition_pattern(suffix, partition_pattern):
    matched_partition_and_key = {}
    # Extract variable names within curly braces, and angular brackets if suffix matches
    matches = re.findall(r"{([^}]+)}|([^{}\/<>]+)|<([^<>]+)>", partition_pattern)
    pattern_parts = []
    for var, text, key in matches:
        if var:
            pattern_parts.append(f"{var}=(.+?)")
        elif text:
            pattern_parts.append(text)
        elif key:
            pattern_parts.append(f"(.+?)")
    pattern_to_be_matched = "/".join(pattern_parts) + "/"
    pattern_match = re.match(pattern_to_be_matched, suffix)
    if pattern_match:
        # Pattern matched
        values = [
            pattern_match.group(i + 1) for i in range(len(pattern_match.groups()))
        ]
        variable_names = re.findall(r"{([^}]+)}|<([^<>]+)>", partition_pattern)
        result_match_dict = {
            var: value
            for var, value in zip(variable_names, values)
            if value is not None
        }

        for result_tuple, value in result_match_dict.items():
            partition, key = result_tuple
            if partition:
                matched_partition_and_key[partition] = value
            elif key:
                matched_partition_and_key[key] = value
    return matched_partition_and_key


def get_next_word(input_string, prefix):
    pattern = prefix + r" (\w+)"
    match = re.search(pattern, input_string)
    if match:
        return match.group(1)
    else:
        return None
