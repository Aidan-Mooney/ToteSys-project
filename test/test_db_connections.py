from moto import mock_aws
import boto3
import pytest
import os
from src.utils.python.db_connections import (
    db_connections_get_secret,
    connect_to_db,
    close_db_connection,
)
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError


class TestDBConnectionGetSecrets:
    @pytest.fixture(scope="function", autouse=True)
    def aws_credentials(self):
        """Mocked AWS Credentials for moto."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

    @pytest.fixture(scope="function")
    def secretsmanager(self, aws_credentials):
        with mock_aws():
            yield boto3.client("secretsmanager", region_name="eu-west-2")

    @pytest.fixture(scope="function", autouse=True)
    def adding_secret(self, secretsmanager):
        secret_name = "secret_test"
        secret_stuff = (
            {
                "user": "project_team",
                "password": "secret_password",
                "host": "example.com",
                "database": "test_db",
                "port": 1234,
            },
        )
        secretsmanager.create_secret(
            Name=secret_name,
            SecretString=str(secret_stuff),
            ForceOverwriteReplicaSecret=True,
        )

    def test_db_connection_read_credentials(self, secretsmanager):
        result = db_connections_get_secret(secretsmanager, "secret_test")
        assert result == {
            "user": "project_team",
            "password": "secret_password",
            "host": "example.com",
            "database": "test_db",
            "port": 1234,
        }

    def test_db_connection_raises_error(self, secretsmanager):
        with pytest.raises(ClientError):
            db_connections_get_secret(secretsmanager, "no_secert_name")


class TestCloseDBConnection:
    def test_close_db_connection_calls_con_close_once(self):
        conn = Mock()
        conn.close.return_value = True
        close_db_connection(conn)
        conn.close.assert_called_once


class TestConnectToDB:
    @pytest.fixture(scope="function", autouse=True)
    def aws_credentials(self):
        """Mocked AWS Credentials for moto."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

    @pytest.fixture(scope="function")
    def secretsmanager(self, aws_credentials):
        with mock_aws():
            yield boto3.client("secretsmanager", region_name="eu-west-2")

    @pytest.fixture(scope="function", autouse=True)
    def adding_secret(self, secretsmanager):
        secret_name = "totesys_db_credentials"
        secret_stuff = (
            {
                "user": "project_team",
                "password": "secret_password",
                "host": "example.com",
                "database": "test_db",
                "port": 1234,
            },
        )
        secretsmanager.create_secret(
            Name=secret_name,
            SecretString=str(secret_stuff),
            ForceOverwriteReplicaSecret=True,
        )

    @mock_aws
    def test_connect_to_db_pg8000_native_connection(self):
        pg8000 = Mock()
        pg8000.native = Mock()
        pg8000.native.Connection = Mock()
        pg8000.native.Connection.return_value = True
        with patch("src.utils.python.db_connections.pg8000.native.Connection") as mock:
            connect_to_db()
        mock.assert_called_with(
            user="project_team",
            password="secret_password",
            database="test_db",
            host="example.com",
            port=1234,
        )
        mock.assert_called_once
