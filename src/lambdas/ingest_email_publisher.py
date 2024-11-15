from boto3 import client
from os import environ

sns_client = client("sns")


def lambda_handler(event={}, context={}):
    email = {
        "TopicArn": environ["sns_topic_arn"],
        "Subject": "🚨 Critical Logs by ToteSys Ingest Function 🚨",
        "Message": "The Ingest lambda function has created critical logs.\n\n🎨 ART NOT CODE 🖼",
    }

    sns_client.publish(**email)
