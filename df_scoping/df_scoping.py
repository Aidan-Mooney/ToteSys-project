import pandas as pd
from os import listdir
from pandas import DataFrame
# do we need to worry about foreign keys in the warehouse? I think not, as we won't be deleting anything.
# use df.dt.time to get the time
# use df.dt.date to get the date


def make_dfs(dir: str, extension: str = ".parquet") -> dict:
    parquet_filenames = listdir(dir)
    dataframes = {}
    for filename in parquet_filenames:
        dataframes[filename[: -len(extension)]] = pd.read_parquet(f"{dir}{filename}")
    return dataframes


def dim_design(design: DataFrame) -> DataFrame:
    df = design["design_id", "design_name", "file_location", "file_name"]
    return df


def dim_transaction(transaction: DataFrame) -> DataFrame:
    df = transaction[
        "transaction_id", "transaction_type", "sales_order_id", "purchase_order_id"
    ]
    return df


def fact_sales_order(sales_order: DataFrame) -> DataFrame:
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


if __name__ == "__main__":
    dataframes = make_dfs("df_scoping/tables/")
    print(fact_sales_order(dataframes["sales_order"]))
