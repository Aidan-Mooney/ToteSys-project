import re


class DateFormatError(Exception):
    pass


def generate_new_entry_query(table_name: str, start_time: str, end_time: str) -> str:
    """
    Return a valid PostgreSQL query string for rows which were modified in the given table between the start_time and end_time.

    :param table_name: The name of the table in the database
    :param start_time: The start of the time range (inclusive)
    :param end_time: The end of the time range (exclusive)
    :raises DateFormatError: if start_time or end_time are in an invalid format. Valid formats include:

        - YYYY-MM-DD
        - YYYY-MM-DD hh:mm:ss
        - YYYY-MM-DD hh:mm:ss.xxx (where xxx are milliseconds)
        - YYYY-MM-DD hh:mm:ss.ususus (where ususus is microseconds)

    :returns query: SQL query which retrieves rows from table table_name updated within the range given.
    """
    time_condition = re.compile(
        r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2}(.\d{3})?(\d{3})?)?$"
    )
    try:
        assert time_condition.match(start_time)
        assert time_condition.match(end_time)
        query = f"""
        SELECT *
        FROM {table_name}
        WHERE '{start_time}' <= last_updated AND last_updated < '{end_time}'
        """
        return query
    except AssertionError:
        raise DateFormatError(
            "start_time and end_time must be correct format: see docstring"
        )
