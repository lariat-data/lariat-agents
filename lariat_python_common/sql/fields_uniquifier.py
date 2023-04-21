import sqlparse
import hashlib
import logging


class BaseFieldUniquifier:
    def uniquify_string(self, string: str):
        if string is None or string == "":
            if string is None:
                logging.warning(
                    f"Received None as input argument. Uniquifier is returning the empty String."
                )
            return ""

        return self._hash_string(string)

    def _hash_string(self, string: str):
        try:
            hash_object = hashlib.sha1(string.encode())
            return str(hash_object.hexdigest())
        except Exception as e:
            logging.error(f"Could not hash the following: {string}")
            raise e


class SQLWhereClauseUniquifier(BaseFieldUniquifier):
    def __init__(self):
        self.COMPARISON_KEYWORDS = {
            "=": self.handle_binary_operator,
            ">": self.handle_binary_operator,
            ">=": self.handle_binary_operator,
            "<": self.handle_binary_operator,
            "<=": self.handle_binary_operator,
            "<>": self.handle_binary_operator,
            "like": self.handle_binary_operator,
            "ilike": self.handle_binary_operator,
            "rlike": self.handle_binary_operator,
            "is": self.handle_binary_operator,
            "between": self.handle_between_operator,
            # "in": "sqlparse already handles the NOT IN case correctly. No need to check for it here."
        }

    def find_rightmost_operand(self, parsed_where_clause, index):
        """
        return the most right operand including `not` keyword
        examples
        X IS NOT 12
        X = 12
        """
        right_operand_index, right_operand = parsed_where_clause.token_next(
            index, skip_ws=True, skip_cm=True
        )
        if str(right_operand).lower() == "not":
            return self.find_rightmost_operand(parsed_where_clause, right_operand_index)

        return right_operand_index, right_operand

    def find_leftmost_operand(self, parsed_where_clause, index):
        """
        return the most left operand including the `not` keyword, in some cases in sql grammer the `not` is before the left operand and sometimes it's after
        examples
        not x = 1
        x not between 12 and 13
        """
        left_operand_index, left_operand = parsed_where_clause.token_prev(
            index, skip_ws=True, skip_cm=True
        )
        left_operand_index2, left_operand2 = parsed_where_clause.token_prev(
            left_operand_index, skip_ws=True, skip_cm=True
        )

        if str(left_operand).lower() == "not" or str(left_operand2).lower() == "not":
            return left_operand_index2, left_operand2

        return left_operand_index, left_operand

    def handle_binary_operator(self, parsed_where_clause, index):
        left_operand_index, _ = self.find_leftmost_operand(parsed_where_clause, index)
        right_operand_index, _ = self.find_rightmost_operand(parsed_where_clause, index)

        return left_operand_index, right_operand_index

    def handle_between_operator(self, parsed_where_clause, index):
        left_operand_index, _ = self.find_leftmost_operand(parsed_where_clause, index)
        right_operand_index = None

        current_right_operand_index = index
        while True:
            (
                current_right_operand_index,
                current_right_operand,
            ) = parsed_where_clause.token_next(
                current_right_operand_index, skip_ws=True, skip_cm=True
            )
            if str(current_right_operand).lower() == "and":
                right_operand_index, _ = parsed_where_clause.token_next(
                    current_right_operand_index, skip_ws=True, skip_cm=True
                )
                break

        return left_operand_index, right_operand_index

    def rebuild_where_clause(self, where_clause, sorted_sublists):
        """
        Takes a where clause part of sql query and list of identifiers
        it orders the identifiers on the original query to match the order in the list
        Example
        input:
            query= "C = 12 AND A = 1 AND B = 2"
            sorted_sublists = ["A = 1", "B = 2", "C = 12"]
        output:
            "A = 1 AND B = 2 AND C = 12
        """
        sublist_index = 0

        new_where_clause = ""
        for token in where_clause.tokens:
            if (
                isinstance(token, sqlparse.sql.Comparison)
                or isinstance(token, sqlparse.sql.Parenthesis)
                or isinstance(token, sqlparse.sql.Identifier)
            ):
                new_where_clause += sorted_sublists[sublist_index]
                sublist_index += 1
            else:
                new_where_clause += str(token)

        return new_where_clause

    def sort_where_clause(self, where_clause):
        """
        Takes a where clause part of sql query and return it sorted based on column_names
        for example:
            input:
                string = "A = 1 AND B = 2 AND C = 12"
            output:
                "A = 1 AND B = 2 AND C = 12

            input:
                (D = 0 AND C = 1) OR (B = 1 AND A = 11)
            output:
                (A = 11 AND B = 1) OR (C = 1 AND D = 0)
        """
        if isinstance(where_clause, sqlparse.sql.Comparison) or isinstance(
            where_clause, sqlparse.sql.Identifier
        ):
            """
            Comparison are simply the simplest form of a where clause
            examples: A = 12, B = 2, C like 'a',...etc
            """
            return str(where_clause)
        else:
            sublists = []
            for subquery in where_clause.get_sublists():
                sublists.append(self.sort_where_clause(subquery))

            sorted_sublists = sorted(sublists)
            return self.rebuild_where_clause(where_clause, sorted_sublists)

    def group_comparisons(self, parsed_where_clause):
        """
        sqlparse does a really job in tokenization but grouping comparisons is not so great
        we want to have each comparison grouped so we can easily sort them
        """
        for index, token in enumerate(parsed_where_clause):
            if str(token).lower() in self.COMPARISON_KEYWORDS:
                group_start, group_end = self.COMPARISON_KEYWORDS[str(token).lower()](
                    parsed_where_clause, index
                )
                parsed_where_clause.group_tokens(
                    sqlparse.sql.Comparison,
                    group_start,
                    group_end,
                )

        return parsed_where_clause

    def group_parenthesis_with_not(self, parsed_where_clause):
        """
        Whenever we find a NOT keyword before a Parenthesis Object we add it to the Parenthesis
        Parentheses are basically a token list defined in sqlparse.sql
        """
        for index, token in enumerate(parsed_where_clause):
            if isinstance(token, sqlparse.sql.Parenthesis):
                prev_token_index, prev_token = parsed_where_clause.token_prev(
                    index, skip_ws=True, skip_cm=True
                )
                if str(prev_token).lower() == "not":
                    parsed_where_clause.group_tokens(
                        sqlparse.sql.Parenthesis, prev_token_index, index
                    )
        return parsed_where_clause

    def uniquify_string(self, string):
        """
        Takes a SQL where clause and return a hash for it, it sort the query before hashing while keeping the comparison operators in mind
        examples:
            string = "A = 1 AND B = 2 AND C = 12"
            string = "C = 12 AND B = 2 AND A = 1"
            These 2 string will produce the same hash,

            string = "A = 1 AND B = 2 AND C = 12"
            string = "C = 12 AND B = 2 AND A = 1"

            this is useful so that same query logic will have the same hash
        """
        if string is None or string == "":
            if string is None:
                logging.warning(
                    f"Received None as input argument. Uniquifier is returning the empty String."
                )
            return ""

        try:
            parsed_where_clause = sqlparse.parse(string)[0]
            parsed_where_clause = self.group_comparisons(
                sqlparse.sql.Statement([i for i in parsed_where_clause.flatten()])
            )
            parsed_where_clause = sqlparse.engine.grouping.group(parsed_where_clause)
            parsed_where_clause = self.group_parenthesis_with_not(parsed_where_clause)
            sorted_where_clause = self.sort_where_clause(parsed_where_clause)

            return super().uniquify_string(sorted_where_clause)
        except Exception as e:
            logging.error(f"Error while parsing and grouping where_clause: {string}")
            raise e


class DelimiterSeparatedListUniquifier(BaseFieldUniquifier):
    def __init__(self, delimiter=",") -> None:
        self.delimiter = delimiter
        super().__init__()

    def uniquify_string(self, string):
        """
        Takes a delimiter separated list as a string and returns a unique hash
        it ignores the sorting and the spaces
        examples:
            string = whatever, something
            string = something,   whatever
        both should give the same hash
        """
        if string is None or string == "":
            if string is None:
                logging.warning(
                    f"Received None as input argument. Uniquifier is returning the empty String."
                )
            return ""
        try:
            splitted_string = [
                column.strip() for column in string.split(self.delimiter)
            ]
            sorted_strings = sorted(splitted_string)
            final_string = self.delimiter.join(sorted_strings)

            return super().uniquify_string(final_string)
        except Exception as e:
            logging.error(f"Error while parsing and grouping where_clause: {string}")
            raise e
