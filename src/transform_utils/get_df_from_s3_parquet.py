from boto3 import client
from io import BytesIO
from pandas import read_parquet


def get_df_from_s3_parquet(filename, bucket_name):
    buffer = BytesIO()
    s3_client = client("s3")
    s3_client.download_fileobj(bucket_name, filename, buffer)
    return read_parquet(buffer)
