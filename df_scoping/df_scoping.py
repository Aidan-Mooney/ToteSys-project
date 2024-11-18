import pandas as pd
from os import listdir
from pandas import DataFrame
# do we need to worry about foreign keys in the warehouse? I think not, as we won't be deleting anything.
# use df.dt.time to get the time
# use df.dt.date to get the date


def make_dfs(dir, extension=".parquet"):
    parquet_filenames = listdir(dir)
    dataframes = {}
    for filename in parquet_filenames:
        dataframes[filename[: -len(extension)]] = pd.read_parquet(f"{dir}{filename}")
    return dataframes


def fact_sales_order(sales_order: DataFrame):
    existing_cols = sales_order[
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
    # rename staff_id
    # add columns for date and time for last_updated and created_at
    return sales_order


if __name__ == "__main__":
    dataframes = make_dfs("df_scoping/tables/")
    # print(dataframes["sales_order"])
    # print(dataframes["design"])
    print(fact_sales_order(dataframes["sales_order"]))
    print(fact_sales_order(dataframes["sales_order"]).dt.date)
