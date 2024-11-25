def create_fact_tables(connect_to_db, close_db_connection):
    
    """if fact table exists: 
        CREATE fact table
        Else """
    
    conn = connect_to_db()
        
    facts_sales_order_query = """CREATE TABLE IF NOT EXISTS facts_sales_order (
                                    sales_record_id SERIAL PRIMARY KEY,
                                    sales_order_id INT NOT NULL,
                                    created_date DATE NOT NULL REFERENCES dim_date(date_id),
                                    created_time TIME NOT NULL,
                                    last_updated_date DATE NOT NULL REFERENCES dim_date(date_id),
                                    last_updated_time TIME NOT NULL,
                                    sales_staff_id INT NOT NULL REFERENCES dim_staff(staff_id),
                                    counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
                                    units_sold INT NULL,
                                    unit_price NUMERIC(10,2) NOT NULL,
                                    currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
                                    design_id INT NOT NULL REFERENCES dim_design(design_id),
                                    agreed_payment_date DATE NOT NULL REFERENCES dim_date(date_id),
                                    agreed_delivery_date DATE NOT NULL REFERENCES dim_date(date_id),
                                    agreed_delivery_location_id INT NOT NULL
                                    ) REFERENCES dim_location(location_id);"""

    facts_payment_query = """CREATE TABLE IF NOT EXISTS facts_payment (
                                payment_record_id SERIAL PRIMARY KEY,
                                payment_id INT NOT NULL,
                                created_date DATE NOT NULL REFERENCES dim_date(date_id),
                                created_time TIME NOT NULL,
                                last_updated_date DATE NOT NULL REFERENCES dim_date(date_id),
                                last_updated_time TIME NOT NULL,
                                transaction_id INT NOT NULL REFERENCES dim_transaction(transaction_id),
                                counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
                                payment_amount NUMERIC NOT NULL,
                                currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
                                payment_type_id INT NOT NULL REFERENCES dim_payment_type(payment_type_id),
                                paid BOOL NOT NULL,
                                payment_date DATE NOT NULL REFERENCES dim_date(date_id);"""
        
    fact_purchase_order_query = """CREATE TABLE IF NOT EXISTS facts_purchase_order (
                                        purchase_record_id SERIAL PRIMARY KEY,
                                        purchase_order_id INT NOT NULL,
                                        created_date DATE NOT NULL REFERENCES dim_date(date_id),
                                        created_time TIME NOT NULL,
                                        last_updated_date DATE NOT NULL REFERENCES dim_date(date_id),
                                        last_updated_time TIME NOT NULL,
                                        staff_id INT NOT NULL REFERENCES dim_staff(staff_id),
                                        counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
                                        item_code VARCHAR(100) NOT NULL,
                                        item_quantity INT NOT NULL,
                                        item_unit_price NUMBERIC NOT NULL,
                                        currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
                                        agreed_payment_date DATE NOT NULL  REFERENCES dim_date(date_id),
                                        agreed_delivery_date DATE NOT NULL  REFERENCES dim_date(date_id),
                                        agreed_delivery_location_id INT NOT NULL
                                        )REFERENCES dim_location(location_id);"""
    
    sql_string = facts_sales_order_query + facts_payment_query + fact_purchase_order_query

    try:
        conn.run(sql_string)
    
    finally:
        close_db_connection(conn)