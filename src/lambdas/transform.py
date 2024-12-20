from boto3 import client
from botocore.exceptions import ClientError
from os import environ
from datetime import datetime
from logging import getLogger

if environ["DEV_ENVIRONMENT"] == "testing":
    from src.utils.python.generate_file_key import generate_file_key
    from src.utils.python.generate_parquet_of_df import generate_parquet_of_df
    from src.utils.python.write_to_s3 import write_to_s3
    from src.utils.python.warehouse import Warehouse
else:
    from generate_file_key import generate_file_key
    from generate_parquet_of_df import generate_parquet_of_df
    from write_to_s3 import write_to_s3
    from warehouse import Warehouse

logger = getLogger(__name__)


def lambda_handler(event, context={}):
    """
    Add the files stored at these paths in the ingest bucket to a Warehouse object. Then, extract the corresponding warehouse tables and place them in the transform s3 bucket.

    :param event: names of updated tables and paths to newest file in the ingest bucket. eg.

    .. code-block :: json
        {
            "address": "address/yadayada.parquet",
            "counterparty": "counterparty/yadayada.parquet",
            "currency": "currency/yadayada.parquet",
            "design": "design/yadayada.parquet"
            ...
        }

    :returns event:

    .. code-block :: json
        {
            "table_name_1": "path_to_dim/fact_table_1_in_tf_bucket",
            "dim_counterparty": "pathtolatestdimcounterparty",
            "fact_payment": "path...",
            ...
        }
    """
    if not event:
        logger.info("No tables to update")
    INGEST_BUCKET_NAME = environ["ingest_bucket_name"]
    TRANSFORM_BUCKET_NAME = environ["transform_bucket_name"]
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
    if "staff" in event:
        ingest_paths.append(environ["static_department_path"])
    if "counterparty" in event and "address" not in event:
        ingest_paths.append(environ["static_address_path"])
    try:
        warehouse = Warehouse(ingest_paths, INGEST_BUCKET_NAME, s3_client)
    except ValueError as v:
        logger.critical(v)
    except AttributeError as a:
        logger.critical(a)
        raise a
    transformed_file_paths = {}
    for table_name in event:
        if table_name in relationships:
            warehouse_table_name = relationships[table_name]
            try:
                df = getattr(warehouse, warehouse_table_name)
                file_key = generate_file_key(warehouse_table_name, current_time)
                write_to_s3(
                    s3_client,
                    TRANSFORM_BUCKET_NAME,
                    file_key=file_key,
                    data=generate_parquet_of_df(df),
                )
                transformed_file_paths[relationships[table_name]] = file_key
            except ClientError as c:
                logger.critical(f"{__name__} failed to write to s3: {c}")
            except AttributeError as a:
                logger.critical(
                    f"Unable to access attribute {warehouse_table_name} from warehouse 🏠🚛: {a}"
                )
    return transformed_file_paths
