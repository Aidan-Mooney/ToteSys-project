import os
from logging import getLogger
from boto3 import client
from botocore.exceptions import ClientError
import json


DEV_ENVIRONMENT = os.environ["DEV_ENVIRONMENT"]
if DEV_ENVIRONMENT == "testing":
    from src.transform_utils.warehouse import Warehouse
    from src.utils.python.get_last_ingest_time import get_latest_filename
else:
    from warehouse import Warehouse
    from get_last_ingest_time import get_latest_filename

s3_client = client("s3")
logger = getLogger(__name__)


def lambda_handler(event, context):
    full_dict = json.loads(os.environ["FULL_LIST"])
    ingest_bucket_name = os.environ["ingest_bucket_name"]
    transform_bucket_name = os.environ["transform_bucket_name"]
    tables_added = event["tables_added"]
    tables_dict = {}
    if full_dict:
        for key, value in full_dict.items():
            if key in tables_added and value not in tables_added:
                tables_added.append(value)
    for table in tables_added:
        latest_file = get_latest_filename(s3_client, ingest_bucket_name, table)
        tables_dict[table] = latest_file
    return_message = {"result": "success", "tables": tables_dict}
    return return_message
