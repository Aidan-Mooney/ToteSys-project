from pandas import DataFrame, Series
from os import environ
from logging import getLogger
from botocore.exceptions import ClientError
from numpy import isnan

if environ["DEV_ENVIRONMENT"] == "testing":
    from src.utils.python.get_df_from_s3_parquet import get_df_from_s3_parquet
else:
    from get_df_from_s3_parquet import get_df_from_s3_parquet

logger = getLogger(__name__)


class Warehouse:
    def __init__(self, list_of_filenames: list[str], bucket_name: str, s3_client):
        """
        Warehouse object expects parquet file from ingest bucket and creates dim- and fact- tables.

        Warning: Only access properties for which you have ingested the relevant dependencies, otherwise will raise a KeyError.

        __init__:
            list_of_filenames: list of file keys in an s3 bucket
            bucket_name: name of the s3 bucket to access ingest files from

        Attributes:
            dim_design (depends on design)
            dim_transation (depends on transaction)
            dim_counterparty (depends on counterparty and address)
            dim_currency (depends on currency)
            dim_payment_type (depends on payment_type)
            dim_location (depends on address)
            dim_staff (depends on staff and department)
            fact_sales_order (depends on sales_order)
            fact_payment (depends on payment)
            fact_purchase_order (depends on purchase_order)
        """
        self.s3_client = s3_client
        self.dataframes = {}
        for filename in list_of_filenames:
            if filename[0:6] == "static":
                table_name = filename[len("static") + 1 : -len(".parquet")]
            else:
                table_name = filename[: filename.index("/")]
            try:
                self.dataframes[table_name] = get_df_from_s3_parquet(
                    self.s3_client, bucket_name, filename
                )
            except ClientError as c:
                logger.critical(
                    f"{__name__} encountered error retrieving file {filename} from bucket {bucket_name}: {c}"
                )
                raise c

    @property
    def dim_design(self) -> DataFrame:
        design = self.dataframes["design"]
        df = design[["design_id", "design_name", "file_location", "file_name"]]
        return none_to_NULL(df)

    @property
    def dim_transaction(self) -> DataFrame:
        transaction = self.dataframes["transaction"]
        df = transaction[
            [
                "transaction_id",
                "transaction_type",
                "sales_order_id",
                "purchase_order_id",
            ]
        ]
        df["sales_order_id"] = format_str_to_int(df["sales_order_id"])
        df["purchase_order_id"] = format_str_to_int(df["purchase_order_id"])
        return none_to_NULL(df)

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
        return none_to_NULL(df)

    @property
    def dim_currency(self) -> DataFrame:
        currency = self.dataframes["currency"]
        names = [
            ["GBP", "Great British Pound"],
            ["USD", "United States Dollar"],
            ["EUR", "Euro"],
        ]
        names_cols = DataFrame(names, columns=["currency_code", "currency_name"])
        currency_cols = currency[["currency_id", "currency_code"]]
        df = currency_cols.merge(names_cols, how="outer", on="currency_code")
        return none_to_NULL(df)

    @property
    def dim_payment_type(self) -> DataFrame:
        payment_type = self.dataframes["payment_type"]
        df = payment_type[["payment_type_id", "payment_type_name"]]
        return none_to_NULL(df)

    @property
    def dim_location(self) -> DataFrame:
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
        return none_to_NULL(df)

    @property
    def dim_staff(self) -> DataFrame:
        staff = self.dataframes["staff"]
        department = self.dataframes["department"]
        staff_cols = staff[
            ["staff_id", "first_name", "last_name", "email_address", "department_id"]
        ]
        department_cols = department[["department_name", "location", "department_id"]]
        df = staff_cols.merge(department_cols, how="inner", on="department_id")
        df.drop(columns=["department_id"], inplace=True)
        return none_to_NULL(df)

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
        df["created_date"] = format_date_for_db(sales_order["created_at"].dt.date)
        df["created_time"] = format_time_for_db(sales_order["created_at"].dt.time)
        df["last_updated_date"] = format_date_for_db(
            sales_order["last_updated"].dt.date
        )
        df["last_updated_time"] = format_time_for_db(
            sales_order["last_updated"].dt.time
        )
        df.rename(columns={"staff_id": "sales_staff_id"}, inplace=True)
        return none_to_NULL(df)

    @property
    def fact_payment(self) -> DataFrame:
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
        df["created_date"] = format_date_for_db(payment["created_at"].dt.date)
        df["created_time"] = format_time_for_db(payment["created_at"].dt.time)
        df["last_updated_date"] = format_date_for_db(payment["last_updated"].dt.date)
        df["last_updated_time"] = format_time_for_db(payment["last_updated"].dt.time)
        print(df)
        return none_to_NULL(df)

    @property
    def fact_purchase_order(self) -> DataFrame:
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
        df["created_date"] = format_date_for_db(purchase_order["created_at"].dt.date)
        df["created_time"] = format_time_for_db(purchase_order["created_at"].dt.time)
        df["last_updated_date"] = format_date_for_db(
            purchase_order["last_updated"].dt.date
        )
        df["last_updated_time"] = format_time_for_db(
            purchase_order["last_updated"].dt.time
        )
        return none_to_NULL(df)

def format_date_for_db(series: Series):
    """
    Map a column of date objects to a column of formatted date strings
    """
    return series.apply(lambda x: x.strftime("%Y-%m-%d"))


def format_time_for_db(series: Series):
    """
    Map a column of time objects to a column of formatted time strings
    """
    return series.apply(lambda x: x.strftime("%H:%M:%S.%f"))


def format_str_to_int(series: Series):
    """
    Map a column of numerical strings/numeric/NaN values to a column of integer strings/NULL values.
    """
    return series.apply(lambda x: "NULL" if isnan(x) else str(int(float(x))))


def none_to_NULL(df: DataFrame):
    """
    Map all None values in df to "NULL" strings
    """
    return df.apply(lambda x: x.apply(lambda y: "NULL" if y is None else str(y)))
