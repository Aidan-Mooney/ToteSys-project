from boto3 import client
from io import BytesIO
from pandas import read_parquet, DataFrame


def get_df_from_s3_parquet(filename: str, bucket_name: str) -> DataFrame:
    """
    Return the pandas DataFrame contained at the location filename inside bucket bucket_name.

    Parameters:
        filename: file key in s3 bucket
        bucket_name: name of bucket
    """
    buffer = BytesIO()
    s3_client = client("s3")
    s3_client.download_fileobj(bucket_name, filename, buffer)
    return read_parquet(buffer)
