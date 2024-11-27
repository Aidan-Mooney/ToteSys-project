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
    - address_line_1: counterparty_legal_address_line_1
    - address_line_2: counterparty_legal_address_line_2
    - district: counterparty_legal_district
    - city: counterparty_legal_city
    - postal_code: counterparty_legal_postal_code
    - country: counterparty_legal_country
    - phone: counterparty_legal_phone_number
  - retains (from counter_party):
    - counterparty_id
    - counterparty_legal_name
  - joined on address.address_id = legal_address_id
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
- dim_staff (depends on staff and department)
- fact_sales_order (depends on sales_order)
- fact_purchase_order (depends on purchase_order)
- fact_payment (depends on payment)

