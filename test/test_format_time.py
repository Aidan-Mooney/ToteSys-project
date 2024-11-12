from src.utils.format_time import format_time
from datetime import datetime

def test_function_receives_datetime_object_and_returns_string():

    date_input = datetime.now()

    output = format_time(date_input)

    assert isinstance(output, str)