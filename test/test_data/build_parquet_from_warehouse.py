from pandas import read_parquet
from os import environ

environ["DEV_ENVIRONMENT"] = "testing"
from src.utils.python.warehouse import Warehouse

parquet_path = "test/test_data/parquet_files"
tables = [
    "address",
    "counterparty",
    "currency",
    "department",
    "design",
    "payment_type",
    "payment",
    "purchase_order",
    "sales_order",
    "staff",
    "transaction",
]
warehouse = Warehouse([], "", "")
for table in tables:
    warehouse.dataframes[table] = read_parquet(f"{parquet_path}/{table}.parquet")

warehouse_tables = [
    "dim_counterparty",
    "dim_currency",
    "dim_design",
    "dim_location",
    "dim_payment_type",
    "dim_staff",
    "dim_transaction",
    "fact_payment",
    "fact_purchase_order",
    "fact_sales_order",
]


def save_parquet(table_name):
    df = getattr(warehouse, table_name)
    df.to_parquet(f"{parquet_path}/{table_name}.parquet")


if __name__ == "__main__":
    for warehouse_table in warehouse_tables:
        save_parquet(warehouse_table)
