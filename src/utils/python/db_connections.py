import boto3
from botocore.exceptions import ClientError
import ast
import pg8000.native


def db_connections_get_secret(sm_client, secret_name: str):
    """
    Retrive the secret stored in AWS SecretsManager with name secret_name.

    Parameters:
    sm_client: AWS SecretsManager client
    secret_name: name of string stored in SecretsManager.

    Structure of the secret as a json object:
        {
            "user": "...",
            "password": "...",
            "host": "...",
            "database": "...",
            "port": ...
        }
    """
    try:
        get_secret_value_response = sm_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = ast.literal_eval(get_secret_value_response["SecretString"])
    return secret


def connect_to_db(secret_name: str = "totesys_db_credentials"):
    """
    Return a pg8000 connection object, connected to the database corresponding to the credentials given.
    Parameters:
    secret_name: name of the secret in SecretsManager which contains the DB credentials. Defaults to "totesys_db_credentials". May only be "totesys_db_credentials" or "totesys_warehouse_credentials"
    """
    assert secret_name in ["totesys_db_credentials", "totesys_warehouse_credentials"]
    secretsmanager = boto3.client("secretsmanager")
    creds = db_connections_get_secret(secretsmanager, secret_name)
    return pg8000.native.Connection(
        user=creds["user"],
        password=creds["password"],
        database=creds["database"],
        host=creds["host"],
        port=int(creds["port"]),
    )


def close_db_connection(conn):
    conn.close()
