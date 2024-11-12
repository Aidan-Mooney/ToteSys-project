from datetime import datetime as dt

def format_time(date_time):

    time_string = str(date_time)
    formatted_string = time_string[:-3]
    return formatted_string