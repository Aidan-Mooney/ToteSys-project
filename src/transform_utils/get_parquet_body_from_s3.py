# in: filename, bucket_name
# out: parquet data
from boto3 import client

s3_client = client("s3")


def get_parquet_body_from_s3(filename, bucket_name):
    body = (
        s3_client.get_object(Bucket=bucket_name, Key=filename)["Body"]
        .read()
        .decode("utf-8")
    )
    return body
