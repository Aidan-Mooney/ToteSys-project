import pandas as pd
from os import listdir
from pandas import DataFrame
from datetime import datetime


class Warehouse:
    def __init__(self, dir: str, extension: str = ".parquet"):
        parquet_filenames = listdir(dir)
        self.dataframes = {}
        for filename in parquet_filenames:
            self.dataframes[filename[: -len(extension)]] = pd.read_parquet(
                f"{dir}/{filename}"
            )

    @property
    def dim_design(self) -> DataFrame:
        design = self.dataframes["design"]
        df = design["design_id", "design_name", "file_location", "file_name"]
        return df

    @property
    def dim_transaction(self) -> DataFrame:
        transaction = self.dataframes["transaction"]
        df = transaction[
            "transaction_id", "transaction_type", "sales_order_id", "purchase_order_id"
        ]
        return df

    @property
    def dim_counterparty(self) -> DataFrame:
        """
        Returns some NaN values - investigate?
        """
        address = self.dataframes["address"]
        counterparty = self.dataframes["counterparty"]
        address_cols = address[
            [
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
            ]
        ]
        address_cols.rename(
            columns={
                "address_line_1": "counterparty_legal_address_line_1",
                "address_line_2": "counterparty_legal_address_line_2",
                "district": "counterparty_legal_district",
                "city": "counterparty_legal_city",
                "postal_code": "counterparty_legal_postal_code",
                "country": "counterparty_legal_country",
                "phone": "counterparty_legal_phone_number",
            },
            inplace=True,
        )
        counterparty_cols = counterparty[["counterparty_id", "counterparty_legal_name"]]
        df = address_cols.join(counterparty_cols)
        return df

    @property
    def dim_currency(self) -> DataFrame:
        currency = self.dataframes["currency"]
        """
        Need to make currency lookup dict using the currency code and then add the name as a column
        """
        df = currency[["currency_id", "currency_code"]]
        return df

    @property
    def fact_sales_order(self) -> DataFrame:
        sales_order = self.dataframes["sales_order"]
        df = sales_order[
            [
                "sales_order_id",
                "design_id",
                "staff_id",
                "counterparty_id",
                "units_sold",
                "unit_price",
                "currency_id",
                "agreed_delivery_date",
                "agreed_payment_date",
                "agreed_delivery_location_id",
            ]
        ]
        df["created_date"] = sales_order["created_at"].dt.date
        df["created_time"] = sales_order["created_at"].dt.time
        df["last_updated_date"] = sales_order["last_updated"].dt.date
        df["last_updated_time"] = sales_order["last_updated"].dt.time
        df.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)
        return df


def dim_date(start_year: int, end_year: int) -> DataFrame:
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
                    date_id = pd.to_datetime(f"{year}-{month:02d}-{day:02d}")
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
    return pd.DataFrame(
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


if __name__ == "__main__":
    warehouse = Warehouse("df_scoping/tables")
    dates = dim_date(2023, 2024)
    with open("df_scoping/output.txt", "w") as f:
        f.write(dates.to_string(header=True, index=False))
