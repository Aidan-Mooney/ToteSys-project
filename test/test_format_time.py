from src.utils.format_time import format_time
from datetime import datetime

def test_function_returns_string():

    date_input = datetime.now()

    output = format_time(date_input)

    assert isinstance(output, str)

def test_function_returns_string_in_correct_date_time_format():

    date_input = datetime(2025, 11, 2, 12, 1, 34, 534654)

    output = format_time(date_input)

    assert len(output) == 26
    assert output == "2025-11-02 12:01:34.534654"

def test_function_can_handle_receiving_end_of_day():

    date_input = datetime(2024, 2, 29, 23, 23, 59, 999999)

    output = format_time(date_input)

    assert len(output) == 26
    assert output == "2024-02-29 23:23:59.999999" 

def test_function_can_handle_receiving_end_of_millisecond():

    date_input = datetime(2024, 2, 29, 22, 23, 59, 999501)

    output = format_time(date_input)

    assert len(output) == 26
    assert output == "2024-02-29 22:23:59.999501"

def test_function_can_accept_datetime_object_where_microseconds_are_exactly_zero():

    date_input = datetime(2025, 11, 12, 12, 12, 11)
    output = format_time(date_input)

    assert len(output) == 26
    assert output == "2025-11-12 12:12:11.000000"
