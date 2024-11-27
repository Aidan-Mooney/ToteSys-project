from io import BytesIO
from pandas import read_parquet, DataFrame


def get_df_from_s3_parquet(s3_client, bucket_name: str, filename: str) -> DataFrame:
    """
    Return the pandas DataFrame contained at the location filename inside bucket bucket_name.

    :param s3_client: boto3 s3 client
    :param bucket_name: name of bucket
    :param filename: file key in s3 bucket
    """
    buffer = BytesIO()
    s3_client.download_fileobj(bucket_name, filename, buffer)
    return read_parquet(buffer)
