from boto3 import client
from src.utils.python.warehouse import Warehouse
from os import environ
from datetime import datetime
from src.utils.python.generate_file_key import generate_file_key
from src.utils.python.generate_parquet_of_df import generate_parquet_of_df
from src.utils.python.write_to_s3 import write_to_s3

INGEST_BUCKET_NAME = environ["ig_bucket_name"]
TRANSFORM_BUCKET_NAME = environ["tf_bucket_name"]
# structure of event should be {
#     table_name : path_to_latest_file,
#     table_name2: path_to_latest_file,
#     ...
#     address: path_to_address,
#     department: path_to_department,
# }


def lambda_handler(event, context={}):
    current_time = datetime.now()
    s3_client = client("s3")
    relationships = {
        "design": "dim_design",
        "transaction": "dim_transaction",
        "counterparty": "dim_counterparty",
        "currency": "dim_currency",
        "payment_type": "dim_payment_type",
        "address": "dim_location",
        "staff": "dim_staff",
        "sales_order": "fact_sales_order",
        "payment": "fact_payment",
        "purchase_order": "fact_purchase_order",
    }
    ingest_paths = []
    for table_name in event:
        ingest_paths.append(event[table_name])
    warehouse = Warehouse(ingest_paths, INGEST_BUCKET_NAME, s3_client)
    for table_name in event:
        if table_name in relationships:
            warehouse_table_name = relationships[table_name]
            df = getattr(warehouse, warehouse_table_name)
            write_to_s3(
                s3_client,
                TRANSFORM_BUCKET_NAME,
                file_key=generate_file_key(table_name, current_time),
                parquet_df=generate_parquet_of_df(df),
            )
