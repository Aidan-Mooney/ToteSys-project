from src.utils.python.create_fact_tables import create_fact_tables
from unittest.mock import Mock

PATCH_PATH = "src.utils.python.create_fact_tables"


def test_create_fact_tables():
    connect_to_db_mock = Mock()
    connect_to_db_mock.return_value = Mock()
    close_db_connection_mock = Mock()
    create_fact_tables(connect_to_db_mock, close_db_connection_mock)
    assert close_db_connection_mock.call_count == 1
    assert (
        connect_to_db_mock.return_value.run.call_args[0][0]
        == """
CREATE TABLE IF NOT EXISTS fact_sales_order (
sales_record_id SERIAL PRIMARY KEY,
sales_order_id INT NOT NULL,
created_date DATE NOT NULL,
created_time TIME NOT NULL,
last_updated_date DATE NOT NULL,
last_updated_time TIME NOT NULL,
sales_staff_id INT NOT NULL,
counterparty_id INT NOT NULL,
units_sold INT NULL,
unit_price NUMERIC(10,2) NOT NULL,
currency_id INT NOT NULL,
design_id INT NOT NULL,
agreed_payment_date DATE NOT NULL,
agreed_delivery_date DATE NOT NULL,
agreed_delivery_location_id INT NOT NULL
);CREATE TABLE IF NOT EXISTS fact_payment (
payment_record_id SERIAL PRIMARY KEY,
payment_id INT NOT NULL,
created_date DATE NOT NULL,
created_time TIME NOT NULL,
last_updated_date DATE NOT NULL,
last_updated_time TIME NOT NULL,
transaction_id INT NOT NULL,
counterparty_id INT NOT NULL,
payment_amount NUMERIC NOT NULL,
currency_id INT NOT NULL,
payment_type_id INT NOT NULL,
paid BOOL NOT NULL,
payment_date DATE NOT NULL
);CREATE TABLE IF NOT EXISTS fact_purchase_order (
purchase_record_id SERIAL PRIMARY KEY,
purchase_order_id INT NOT NULL,
created_date DATE NOT NULL,
created_time TIME NOT NULL,
last_updated_date DATE NOT NULL,
last_updated_time TIME NOT NULL,
staff_id INT NOT NULL,
counterparty_id INT NOT NULL,
item_code VARCHAR(100) NOT NULL,
item_quantity INT NOT NULL,
item_unit_price NUMBERIC NOT NULL,
currency_id INT NOT NULL,
agreed_payment_date DATE NOT NULL,
agreed_delivery_date DATE NOT NULL,
agreed_delivery_location_id INT NOT NULL
);
""".strip()
    )
