from pandas import DataFrame, to_datetime
from datetime import datetime


def dim_date(start_year: int, end_year: int) -> DataFrame:
    """
    Return a DataFrame containing all the days between the years provided: 1st Jan start_year <= day < 1st Jan end_year.

    :param start_year: year to begin the dim_date table, on the 1st of Jan
    :param end_year: year to end the dim_date table, on the 31st of Dec
    :returns dim_date DataFrame:
    """
    quarters = {i: int(4 * i / 12) + 1 for i in range(0, 12)}
    month_names = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    day_names = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    date_lines = []
    for year in range(start_year, end_year):
        for month in range(1, 13):
            for day in range(1, 32):
                try:
                    date_id = to_datetime(f"{year}-{month:02d}-{day:02d}")
                    day_object = datetime(year, month, day)
                    day_of_week = day_object.weekday()  # monday is 0 and sunday is 6
                    day_name = day_names[day_of_week]
                    month_name = month_names[month - 1]
                    quarter = quarters[month - 1]
                    date_lines.append(
                        [
                            date_id,
                            year,
                            month,
                            day,
                            day_of_week,
                            day_name,
                            month_name,
                            quarter,
                        ]
                    )
                except ValueError:
                    continue
    dim_date = DataFrame(
        date_lines,
        columns=[
            "date_id",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "month_name",
            "quarter",
        ],
    )
    dim_date["date_id"] = format_date_for_db(dim_date["date_id"])
    return dim_date


def format_date_for_db(series):
    return series.apply(lambda x: x.strftime("%Y-%m-%d"))


if __name__ == "__main__":
    df = dim_date(2020, 2026)
    df.to_parquet("data/dim_date.parquet")
    df.to_parquet("test/test_data/parquet_files/dim_date.parquet")
