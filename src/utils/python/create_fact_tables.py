def create_fact_tables(connect_to_db, close_db_connection):
    """
    1. connect to the warehouse db
    2. create fact sales order query created if it doesn't exist
    3. create fact payment query created if it doesn't exist
    4. create fact purchase order query created if it doesn't exist
    5. run the query
    6. close the database connection

    :param connect_to_db: connects to the database
    :param close_db_connection: closes the database connection


    """

    conn = connect_to_db(secret_name="totesys_warehouse_credentials")

    facts_sales_order_query = """
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
);
""".strip()

    facts_payment_query = """
CREATE TABLE IF NOT EXISTS fact_payment (
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
);
""".strip()

    fact_purchase_order_query = """
CREATE TABLE IF NOT EXISTS fact_purchase_order (
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

    sql_string = (
        facts_sales_order_query + facts_payment_query + fact_purchase_order_query
    )

    try:
        conn.run(sql_string)

    finally:
        close_db_connection(conn)
