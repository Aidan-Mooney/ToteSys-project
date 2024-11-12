from moto import mock_aws
import boto3
import pytest
import os
from src.utils.db_connections import db_connections_get_secret, connect_to_db
import json

class TestDBConnectionGetSecrets:
    @pytest.fixture(scope="function", autouse=True)
    def aws_credentials(self):
        """Mocked AWS Credentials for moto."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

    @pytest.fixture(scope='function')
    def secretsmanager(self, aws_credentials):
        with mock_aws():
            yield boto3.client('secretsmanager', region_name='eu-west-2')

    @pytest.fixture(scope='function',autouse=True)
    def adding_secret(self, secretsmanager):
        secret_name = 'secret_test'
        secret_stuff = {
            'user': 'project_team',
            'password': 'secret_password',
            'host': 'example.com',
            'database': 'test_db',
            'port' : 1234
        },
        secretsmanager.create_secret(
            Name=secret_name,
            SecretString=str(secret_stuff),
            ForceOverwriteReplicaSecret=True)

    def test_db_connection_read_credentials(self, secretsmanager):   
        result = db_connections_get_secret(secretsmanager, 'secret_test')
        assert result == {'user': 'project_team', 
                          'password': 'secret_password', 
                          'host': 'example.com', 
                          'database': 'test_db', 
                          'port': 1234}
