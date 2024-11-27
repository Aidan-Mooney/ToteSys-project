from datetime import datetime

"""Description
input

table_name, end_time
process

convert end time to yyyy/mm/dd/hhMMss and add it to table_name/
return

table_name/yyyy/mm/dd/hhMMss"""


def generate_file_key(table_name: str, end_time: datetime, extension: str = "parquet"):
    """returns tablename and date string from table and datetime object"""
    date_str = end_time.strftime("%Y/%m/%d/%H%M%S%f")
    return f"{table_name}/{date_str}.{extension}"
