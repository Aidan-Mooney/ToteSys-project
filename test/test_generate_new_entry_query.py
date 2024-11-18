from src.utils.python.generate_new_entry_query import generate_new_entry_query, DateFormatError
from pytest import mark, raises


@mark.it("test query inserts table_name, start_time and end_time into query.")
def test_1():
    table_name = "test_table_name"
    start_time = "2024-11-12 12:13:52.962"
    end_time = "2024-11-13 12:13:52.962"
    output = """
        SELECT *
        FROM test_table_name
        WHERE '2024-11-12 12:13:52.962' <= last_updated AND last_updated < '2024-11-13 12:13:52.962'
        """
    assert generate_new_entry_query(table_name, start_time, end_time) == output


@mark.it("test invalid start time raises DateFormatError")
def test_2():
    table_name = "test_table_name"
    start_time = "I am invalid"
    end_time = "2024-11-13 12:13:52.962"
    with raises(DateFormatError) as err:
        generate_new_entry_query(table_name, start_time, end_time)
    assert (
        str(err.value)
        == "start_time and end_time must be correct format: see docstring"
    )


@mark.it("test invalid end time raises DateFormatError")
def test_3():
    table_name = "test_table_name"
    start_time = "2024-11-13 12:13:52.962"
    end_time = "I am invalid"
    with raises(DateFormatError) as err:
        generate_new_entry_query(table_name, start_time, end_time)
    assert (
        str(err.value)
        == "start_time and end_time must be correct format: see docstring"
    )


@mark.it("test date with no time")
def test_4():
    table_name = "test_table_name"
    start_time = "2024-11-12"
    end_time = "2024-11-13"
    output = """
        SELECT *
        FROM test_table_name
        WHERE '2024-11-12' <= last_updated AND last_updated < '2024-11-13'
        """
    assert generate_new_entry_query(table_name, start_time, end_time) == output


@mark.it("test date and time with no milliseconds")
def test_5():
    table_name = "test_table_name"
    start_time = "2024-11-12 12:13:52"
    end_time = "2024-11-13 12:13:52"
    output = """
        SELECT *
        FROM test_table_name
        WHERE '2024-11-12 12:13:52' <= last_updated AND last_updated < '2024-11-13 12:13:52'
        """
    assert generate_new_entry_query(table_name, start_time, end_time) == output


@mark.it("test date and time with microseconds")
def test_6():
    table_name = "test_table_name"
    start_time = "2024-11-12 12:13:52.324567"
    end_time = "2024-11-13 12:13:52.123456"
    output = """
        SELECT *
        FROM test_table_name
        WHERE '2024-11-12 12:13:52.324567' <= last_updated AND last_updated < '2024-11-13 12:13:52.123456'
        """
    assert generate_new_entry_query(table_name, start_time, end_time) == output