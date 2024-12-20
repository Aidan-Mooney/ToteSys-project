from boto3 import client
from re import compile
from datetime import datetime


def get_latest_filename(s3_client, bucket_name: str, table_name: str) -> str | None:
    """
    Return the file in the bucket bucket_name with the prefix table_name which has the "biggest" name, ie. the name containing the latest timestamp.

    :param s3_client: boto3 client for accessing s3
    :param bucket_name: name of the s3 bucket to retrieve filename from
    :param table_name: name of the table to get the latest filename
    :returns filename: filename of latest file for table table_name.
    """
    try:
        objs = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{table_name}/")[
            "Contents"
        ]
    except KeyError:
        return None
    filenames = [obj["Key"] for obj in objs]
    most_recent_filename = max(filenames)[len(table_name) + 1 :]
    return most_recent_filename


def get_last_ingest_time(bucket_name: str, table_name: str) -> datetime | None:
    """
    Return a datetime object corresponding to the key of the most-recently-modified file in bucket bucket_name with the prefix table_name.

    :param bucket_name: name of the bucket to retrieve filenames from
    :param table_name: name of table to retrieve the last ingest time for
    :returns datetime: datetime object of the last ingest time for table table_name
    """
    s3_client = client("s3")
    filename = get_latest_filename(s3_client, bucket_name, table_name)
    if not filename:
        return filename
    else:
        regex = compile(r"(\d{4})/(\d{2})/(\d{2})/(\d{2})(\d{2})(\d{2})(\d{6})").match(
            filename
        )
        time = {
            "year": regex.group(1),
            "month": regex.group(2),
            "day": regex.group(3),
            "hour": regex.group(4),
            "minute": regex.group(5),
            "second": regex.group(6),
            "microsecond": regex.group(7),
        }
    time = {key: int(time[key]) for key in time}
    return datetime(**time)
