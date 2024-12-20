# Terrific Totes 

## Transform Process

## Overview

### `transform.py`

The transform process aims to:

- Read the parquet files from the [ingest process](ingest.md)
- Process and transform data to the correct star schema
- Write parquet files for upload to the data warehouse

#### Function
- `lambda_handler`

#### Purpose
- Expects an event containing a dictionary with the table from totesysDB that has been updated
- Adds the files stored at these paths in the ingest bucket to a Warehouse object
- It then extracts the corresponding warehouse tables and places them in the transform s3 bucket
- Returns a dictionary of the dim tables and fact tables and their corresponding keys that have been added to the s3 transform bucket

#### Inputs
- `event` Mandatory, no default. Structure of event: 
  - `{"address": "address/yadayada.parquet","counterparty": "counterparty/yadayada.parquet", "currency": "currency/yadayada.parquet","design": "design/yadayada.parquet"...}`

- `context` Mandatory, no default. Metadata about the lambda handler.

#### Outputs
- A dictionary containing the table names as keys and file_key as value.
- `{"table_name_1": "path_to_dim/fact_table_1_in_tf_bucket",  "dim_counterparty": "pathtolatestdimcounterparty", "fact_payment": "path...", ...}`

#### Logging
- `CRITICAL` when each function fails fatally

## Package

### `warehouse.py`

#### Purpose
Warehouse object expects parquet file from ingest bucket and creates dim- and fact- tables.

**Warning**: Only access properties for which you have ingested the relevant dependencies, otherwise will raise a KeyError.

#### Classes
- `Warehouse`

#### Constructor
- `__init__`

#### Inputs
- `list_of_filenames` list of file keys in an s3 bucket

- `bucket_name` name of the s3 bucket to access ingest files from

#### Properties
Each produces a pandas dataframe representing a dimension or fact table for the data warehouse

- `dim_design` (depends on design)
  - retains:
    - design_id
    - design_name
    - file_location
    - file_name

- `dim_transation` (depends on transaction)
  - retains: 
    - transaction_id
    - transaction_type
    - sales_order_id
    - purchase_order_id

- `dim_counterparty` (depends on counterparty and address)
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

- `dim_currency` (depends on currency)
  - creates fixed mapping for currency_code to currency name
  - retains:
    - currency_id
    - currenct_code

- `dim_payment_type` (depends on payment_type)
  - retains:
    - payment_type_id
    - payment_type_name

- `dim_location` (depends on address)
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

- `dim_staff` (depends on staff and department)
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

- `fact_sales_order` (depends on sales_order)
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

- `fact_payment` (depends on payment)
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

- `fact_purchase_order` (depends on purchase_order)
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

  #### Static Functions
  - `format_date_for_db`
    - takes a Series as a parameter, which is a column that has a datetime.date format, and casts it to a string in the format of `%Y-%m-%d` 

  - `format_time_for_db`
    - takes a Series as a parameter, which is a column that has a datetime.time format, and casts it to a string in the format of `%H:%M:%S.%f` 

  - `format_str_to_int`
    - takes a Series as a parameter, which is a column that has an id reference, and checks if a value is a NaN and replaces it with `NULL`, otherwise if the value is a float it get casted to a int before casting it as a string.

  - `none_to_NULL`
    -  takes the whole DataFrame and checks if there is any `None` value and changes it to `NULL`
___

### `get_df_from_parquet.py`

#### Purpose
Returns the DataFrame of the parquet data from the corresponding s3 bucket and key.

#### Function
- `get_df_from_parquet`

#### Purpose
- Gets the parquet data from the corresponding table name and file key.
- Returns the DataFrame from the parquet data

#### Inputs
- `s3_client` Mandatory, no default. Takes a boto3.client s3 object.

- `bucket_name` Mandatory, no default. Takes a string representing the name of the bucket.

- `filename` Mandatory, no default. Takes a string representing the key of the file the is going to be accessed.

#### Outputs
- `read_parquet(buffer)` Returns a DataFrame of the paraquet data accessed by the s3 client.

#### Logging
- Currently none

___

### `generate_parquet_of_df.py`

#### Purpose
Returns the parquet data from the corresponding DataFrame.

#### Function
- `generate_parquet_of_df`

#### Purpose
- Takes the DataFrame from the corrsponding table
- Converts it to parquet data ready to be uploaded to an s3 bucket.

#### Inputs
- `df` Mandatory, no default. Takes the DataFrame that will be .

#### Outputs
- `out_buffer.getvalue()` Returns a paraquet data from the corresponding DataFrame.

#### Logging
- Currently none

___

### `dim_date.py`
Creates a DataFrame of dates with information ready to be analysed.

#### Function
- `dim_date`

#### Purpose
- Creates dim_date DataFrame between two specified years which contains the following columns:
  - data_id
  - year
  - month
  - day
  - day_of_week
  - day_name
  - month_name
  - quarter

#### Inputs
- `start_year` Mandatory, no default. Takes an int for the year to start the data.

- `end_year` Mandatory, no default. Takes an int for the year to end the the data

#### Outputs
- `dim_date` Returns a DataFrame containing all the breakdown of dates between the two years given.

#### Logging
- Currently none

___

#### Function 
- `format_date_for_db`

#### Purpose
- Takes a Panda Series as a parameter, which is a column that has a datetime.date format, and casts it to a string in the format of `%Y-%m-%d` 

#### Inputs
- `series` Mandatory, no default. Takes a Panda Series of datas.

#### Outputs
- Returns the Series in the correct format.

#### Logging
- Currently none