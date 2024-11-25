from src.dim_date import dim_date
from pandas import DataFrame
from pytest import mark
import numpy


class TestDimDate:
    @mark.it("checks if the return value is an instance of a dataframe")
    def test_1(self):
        df = dim_date(2023, 2024)
        assert isinstance(df, DataFrame)

    @mark.it("checks if it returns the correct columns")
    def test_2(self):
        df = dim_date(2023, 2024)
        date_cols = df.columns
        assert len(date_cols) == 8
        assert date_cols[0] == "date_id"
        assert date_cols[1] == "year"
        assert date_cols[2] == "month"
        assert date_cols[3] == "day"
        assert date_cols[4] == "day_of_week"
        assert date_cols[5] == "day_name"
        assert date_cols[6] == "month_name"
        assert date_cols[7] == "quarter"

    @mark.it("checks if it returns the correct number of rows in a year")
    def test_3(self):
        df = dim_date(2023, 2024)
        assert len(df) == 365
        df = dim_date(2024, 2025)
        assert len(df) == 366

    @mark.it("checks if it returns the correct datatype")
    def test_4(self):
        df = dim_date(2023, 2024)
        assert isinstance(df.loc[0]["date_id"], str)
        assert isinstance(df.loc[0]["year"], numpy.int64)
        assert isinstance(df.loc[0]["month"], numpy.int64)
        assert isinstance(df.loc[0]["day"], numpy.int64)
        assert isinstance(df.loc[0]["day_of_week"], numpy.int64)
        assert isinstance(df.loc[0]["day_name"], str)
        assert isinstance(df.loc[0]["month_name"], str)
        assert isinstance(df.loc[0]["quarter"], numpy.int64)

    @mark.it("checks if it returns the correct day values")
    def test_5(self):
        day = dim_date(2023, 2024).loc[20]
        assert day["date_id"] == "2023-01-21"
        assert day["year"] == 2023
        assert day["month"] == 1
        assert day["day"] == 21
        assert day["day_of_week"] == 5
        assert day["day_name"] == "Saturday"
        assert day["month_name"] == "January"
        assert day["quarter"] == 1
