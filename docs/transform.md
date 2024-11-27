# Terrific Totes - Transform Process

## Overview

The transform process aims to:

- Read the parquet files from the [ingest process](ingest.md)
- Process and transform data to the correct star schema
- Write parquet files for upload to the data warehouse

### Package

#### `warehouse.py`

##### Purpose

Warehouse object expects parquet file from ingest bucket and creates dim- and fact- tables.
**Warning**: Only access properties for which you have ingested the relevant dependencies, otherwise will raise a KeyError.

##### Classes

`Warehouse`

###### `__init__`

`list_of_filenames` list of file keys in an s3 bucket
`bucket_name` name of the s3 bucket to access ingest files from

###### Properties

Each produces a pandas dataframe representing a dimension or fact table for the data warehouse

- dim_design (depends on design)
  - retains:
    - design_id
    - design_name
    - file_location
    - file_name

- dim_transation (depends on transaction)
  - retains: 
    - transaction_id
    - transaction_type
    - sales_order_id
    - purchase_order_id

- dim_counterparty (depends on counterparty and address)
  - retains(from address):
    - address_id
    - address_line_1
    - address_line_2
    - district
    - city
    - postal_code
    - country
    - phone
  - remaps:
    - address_line_1:             
      - counterparty_legal_address_line_1
    - address_line_2: 
      - counterparty_legal_address_line_2
    - district: 
      - counterparty_legal_district
    - city: 
      - counterparty_legal_city
    - postal_code: 
      - counterparty_legal_postal_code
    - country: 
      - counterparty_legal_country
    - phone: 
      - counterparty_legal_phone_number
  - retains (from counter_party):
    - counterparty_id
    - counterparty_legal_name
  - joined on address.address_id = counterparty.legal_address_id

- dim_currency (depends on currency)
  - creates fixed mapping for currency_code to currency name
  - retains:
    - currency_id
    - currenct_code

- dim_payment_type (depends on payment_type)
  - retains:
    - payment_type_id
    - payment_type_name

- dim_location (depends on address)
  - retains:
    - address_id
    - address_line_1
    - address_line_2
    - district
    - city
    - postal_code
    - country
    - phone
  - remaps:
    - address_id: 
      - location_id

- dim_staff (depends on staff and department)
  - retains (from staff):
    - staff_id
    - first_name
    - last_name
    - email_address
    - department_id 
  - retains (from department):
    - department_id
    - department_name
    - location
  - joined on staff.department = department.department_id

- fact_sales_order (depends on sales_order)
  - retains:
    - sales_order_id
    - created_at
    - last_updated_at
    - design_id
    - staff_id
    - counterparty_id
    - units_sold
    - unit_price
    - currency_id 
    - agreed_delivery_date
    - agreed_payment_date
    - agreed_delivery_location_id
  - remaps:
    - staff_id:
      - sales_staff_id
    - created_at:
      - created_date
      - created_time
    - last_updated_at:
      - last_updated_date
      - last_updated_time

- fact_payment (depends on payment)
  - retains:
    - payment_id
    - created_at
    - last_updated_at
    - transaction_id
    - counterparty_id
    - payment_amount
    - currency_id
    - payment_type_id
    - paid
    - payment_date
  - remaps:
    - created_at:
      - created_date
      - created_time
    - last_updated_at:
      - last_updated_date
      - last_updated_time

- fact_purchase_order (depends on purchase_order)
  - retains:
    - purchase_order_id
    - created_at
    - last_updated_at
    - staff_id
    - counterparty_id
    - item_code
    - item_quantity
    - item_unit_price
    - currency_id
    - agreed_delivery_date
    - agreed_payment_date
    - agreed_delivery_location_id
  - remaps:
    - created_at:
      - created_date
      - created_time
    - last_updated_at:
      - last_updated_date
      - last_updated_time

  - format_date_for_db
    - takes a series/column of the swaps it to the time

  - formate_time_for_db

#### `get_df_from_parquet.py`