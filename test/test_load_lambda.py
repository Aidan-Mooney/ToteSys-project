from os import environ
from unittest.mock import call, patch

from pytest import mark

environ["DEV_ENVIRONMENT"] = "testing"
from src.lambdas.load import lambda_handler

patch_path = "src.lambdas.load"


@mark.it("calls generate warehouse query with correct table names and path")
def test_1():
    test_event = {"table_name_1": "path_1", "table_name_2": "path_2"}
    with patch(f"{patch_path}.generate_warehouse_query") as gwq_mock:
        with patch(f"{patch_path}.query_db"):
            with patch(f"{patch_path}.client") as client_mock:
                lambda_handler(test_event, {})

    for table_name in test_event:
        assert (
            call(table_name, test_event[table_name], client_mock())
            in gwq_mock.call_args_list
        )
