def format_time(date_time):


    date_time_string = date_time.strftime(f"%Y-%m-%d %H:%M:%S.%f")
    formatted_time = date_time_string[:-3]

    return formatted_time

    # millisecond = str(math.floor(int(date_time.strftime('%f'))))[:3]
    # formatted_time = date_time.strftime(f"%Y-%m-%d %H:%M:%S.{millisecond}")
    # return formatted_time