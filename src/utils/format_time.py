def format_time(date_time):


    date_time_string = date_time.strftime(f"%Y-%m-%d %H:%M:%S.%f")
    formatted_time = date_time_string[:-3]

    return formatted_time
