from boto3 import client
from re import compile
from datetime import datetime


def get_latest_filename(s3_client, bucket_name, table_name):
    """
    Return the file in the bucket bucket_name with the prefix table_name which has the "biggest" name, ie. the name containing the latest timestamp.
    """
    objs = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{table_name}/")[
        "Contents"
    ]
    most_recent_filename = max(objs, key=lambda x: x["Key"])["Key"][
        len(table_name) + 1 :
    ]
    return most_recent_filename


def get_last_ingest_time(bucket_name, table_name):
    """
    Return a datetime object corresponding to the key of the most-recently-modified file in bucket bucket_name with the prefix table_name.
    """
    s3_client = client("s3")
    filename = get_latest_filename(s3_client, bucket_name, table_name)
    regex = compile(r"(\d\d\d\d)/(\d\d)/(\d\d)/(\d\d)(\d\d)(\d\d)").match(filename)
    time = {
        "year": regex.group(1),
        "month": regex.group(2),
        "day": regex.group(3),
        "hour": regex.group(4),
        "minute": regex.group(5),
        "second": regex.group(6),
    }
    time = {key: int(time[key]) for key in time}
    return datetime(**time)
