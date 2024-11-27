from datetime import datetime


def generate_file_key(
    table_name: str, end_time: datetime, extension: str = "parquet"
) -> str:
    """
    Return a string to be used as a file key in s3 bucket. Is of fromat yyyy/mm/dd/

    :param table_name: name of table to generate file key for
    :param end_time: time to be used in the file key
    :param extension: file extension to be added to the end of the filename
    """
    date_str = end_time.strftime("%Y/%m/%d/%H%M%S%f")
    return f"{table_name}/{date_str}.{extension}"
