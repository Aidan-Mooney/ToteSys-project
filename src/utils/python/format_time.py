from datetime import datetime


def format_time(date_time: datetime):
    """
    Return a string-formatted representation of the datetime object date_time of the format YY-MM-DD hh:mm:ss.ususus

    Parameters:
    - date_time: datetime object
    """
    date_time_string = date_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    return date_time_string
