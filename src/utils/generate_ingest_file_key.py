"""Description
input

table_name, end_time
process

convert end time to yyyy/mm/dd/hhMMss and add it to table_name/
return

table_name/yyyy/mm/dd/hhMMss"""


def generate_ingest_file_key(table_name, end_time):
    """returns tablename and date string from table and datetime object"""
    date_str = end_time.strftime("%Y/%m/%d/%H%M%S")
    return f"{table_name}/{date_str}"
