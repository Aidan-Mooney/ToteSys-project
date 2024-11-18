from os import listdir
from pandas import DataFrame, read_parquet


class Warehouse:
    def __init__(self, dir: str, extension: str = ".parquet"):
        parquet_filenames = listdir(dir)
        self.dataframes = {}
        for filename in parquet_filenames:
            self.dataframes[filename[: -len(extension)]] = read_parquet(
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
        address = self.dataframes["address"]
        counterparty = self.dataframes["counterparty"]
        address_cols = address[
            [
                "address_id",
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
        counterparty_cols = counterparty[
            ["counterparty_id", "counterparty_legal_name", "legal_address_id"]
        ]
        counterparty_cols.rename(
            columns={"legal_address_id": "address_id"}, inplace=True
        )
        df = address_cols.merge(counterparty_cols, how="inner", on="address_id")
        df.drop(columns=["address_id"], inplace=True)
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
    def dim_payment_type(self):
        payment_type = self.dataframes["payment_type"]
        df = payment_type[["payment_type_id", "payment_type_name"]]
        return df

    @property
    def dim_location(self):
        location = self.dataframes["address"]
        df = location[
            [
                "address_id",
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
            ]
        ]
        df.rename(columns={"address_id": "location_id"}, inplace=True)
        return df

    @property
    def dim_staff(self):
        staff = self.dataframes["staff"]
        department = self.dataframes["department"]
        staff_cols = staff[
            ["staff_id", "first_name", "last_name", "email_address", "department_id"]
        ]
        department_cols = department[["department_name", "location", "department_id"]]
        df = staff_cols.merge(department_cols, how="inner", on="department_id")
        df.drop(columns=["department_id"], inplace=True)
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

    @property
    def fact_payment(self):
        """
        column payment_record_id to be added by PostgreSQL as a serial primary key. Otherwise we'd have to keep track of the previous run's last primary key.
        """
        payment = self.dataframes["payment"]
        df = payment[
            [
                "payment_id",
                "transaction_id",
                "counterparty_id",
                "payment_amount",
                "currency_id",
                "payment_type_id",
                "paid",
                "payment_date",
            ]
        ]
        df["created_date"] = payment["created_at"].dt.date
        df["created_time"] = payment["created_at"].dt.time
        df["last_updated_date"] = payment["last_updated"].dt.date
        df["last_updated_time"] = payment["last_updated"].dt.time
        return df

    @property
    def fact_purchase_order(self):
        """
        column purchas_order_id to be added by PostgreSQL as a serial primary key. Otherwise we'd have to keep track of the previous run's last primary key.
        """
        purchase_order = self.dataframes["purchase_order"]
        df = purchase_order[
            [
                "purchase_order_id",
                "staff_id",
                "counterparty_id",
                "item_code",
                "item_quantity",
                "item_unit_price",
                "currency_id",
                "agreed_delivery_date",
                "agreed_payment_date",
                "agreed_delivery_location_id",
            ]
        ]
        df["created_date"] = purchase_order["created_at"].dt.date
        df["created_time"] = purchase_order["created_at"].dt.time
        df["last_updated_date"] = purchase_order["last_updated"].dt.date
        df["last_updated_time"] = purchase_order["last_updated"].dt.time
        return df


if __name__ == "__main__":
    warehouse = Warehouse("test/test_data/parquet_files")
    with open("output.txt", "w") as f:
        warehouse.fact_purchase_order.to_string(f)
