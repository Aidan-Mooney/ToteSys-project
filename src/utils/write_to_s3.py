def write_to_s3(s3_client, bucket_name: str, file_key: str, data: str):
    """
    Write the provided data to a file with filename file_key in the bucket bucket_name in the s3 client s3_client.
    Parameters:
    s3_client: Boto3.client("s3") object
    bucket_name: name of the bucket (must already exist)
    file_key: name of the file in the bucket, including directory and file extension
    data: contents of the file.
    """
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=data)
