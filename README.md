# ToteSys Data Engineering Project

## Introduction

## Table of Content
- Usage
- Project Components
- Python
- Terraform
- CICD

## Prerequisites
- You must have AWS credentials set up in your terminal (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`).
- You must have the credentials for a database with populated tables matching this schema: https://dbdiagram.io/d/SampleDB-6332fecf7b3d2034ffcaaa92
- You must have the credentials for a data warehouse with unpopulated tables matching this schema: https://dbdiagram.io/d/RevisedDW-63a19c5399cb1f3b55a27eca
- An email for logging problems.
## Usage

### Step 1: Credentials
Prepare credentials for the project so that the code can access the necessary databases and email for logging. Credentials needed includes the original database credentials, warehouse credentials and email for logging.
#### Database Credentials
- Rename `db_credentials.json.example` to `db_credentials.json`
- Modify the values that say `CHANGE_ME` to your own details
#### Warehouse Credentials
- Rename `warehouse_credentials.json.example` to `warehouse_credentials.json`
- Modify the values that say `CHANGE_ME` to your own details
#### Email
- Rename `email.txt.example` to `email.txt`
- Modify the text from `email@example.com` to your own email of choice

### Step 2: Deployment
#### Installing Requirements
Run `make install-requirements`. This will set up your environment and create a dependencies folder to be deployed as a lambda layer.
#### Testing Code
Next the code will be tested to make sure all the code is running correctly, is safe and conforms to pep8.
- Run `make install-dev-tools`
- Run `make security-checks`
- Run `make check-pep8-compliance`
- Run `make run-pytest`

Make sure all these tests are successful before continuing.

#### Spinning Up Immutable Infastructure
Navigate to the `immutable-terraform` folder by running `cd immutable-terraform` in the root directory. Then run
- `init terraform`
- `plan terraform`
If plan terraform is successful then run `apply terraform -auto-approve`. Finally navigate back to the root directory with `cd ..`

#### Spinning Up Main-Terraform Infastructure
Navigate to the `main-terraform` folder by running `cd main-terraform` in the root directory. Then run
- `init terraform`
- `plan terraform`
If plan terraform is successful then run `apply terraform -auto-approve`. Finally navigate back to the root directory with `cd ..`

## Project Components - Python

## Ingest Process

## Overview

### `ingest.py`

The ingest process aims to:

- Copy data from the ToteSys database
- Convert this data into Parquet format
- Save this data into an S3 bucket with an identifiable file name

#### Function
- `lambda_handler`

#### Purpose
- Gets the start_time from `get_last_ingest_time`
- `end_time` is the time at the start of the run
- generates a SQL query for each table name
- queries the database
- converts the data to parquet
- adds it to the s3 bucket

#### Inputs
- `event` Mandatory, no default. Structure of event: `event = {"tables_to_query": ["table_name",...]}`

- `context` Mandatory, no default. Metadata about the lambda handler.

#### Outputs
- A dictionary containing the table names as keys and file_key as value.

-  `{'table_name_1' : 'table_name/yyyy/mm/dd/hhmmssmmmmmm.parquet','table_name_2' : 'table_name/yyyy/mm/dd/hhmmssmmmmmm.parquet', ... }`

#### Logging
- `INFO` when each function is successful
- `CRITICAL` when each function fails fatally
- `WARNING` when query_db returns an empty list

## Packages

### `db_connections.py`

#### Purpose

This package provides the ability to connect to the Terrific Totes database and to close the connection.  It also manages the secret retrieval needed to connect.

#### Function
- `db_connections_get_secret`

#### Purpose
- Returns a dictionary containing the requested secret.

#### Inputs
- `client` Mandatory, no default. Takes a boto3.client secretsmanager object.

- `secret_name` Mandatory, no default. Takes a string representing the name of the secret to be retrieved.

#### Outputs
- `secret` Returns a dictionary containing the secret retrieved

#### Logging
- Currently none

___

#### Function
- `connect_to_db`

#### Purpose
- Returns an authenticated pg8000 connection object for the totesysDB

#### Inputs
- `None`

#### Outputs
- `connection` Returns a pg8000 native connection object

#### Logging
- Currently none

___

#### Function

- `close_db_connection`

#### Purpose
- Closes the an existing database connection.

#### Inputs
- `conn` Mandatory, no default. Takes a pg8000 native connection object

#### Outputs
- `None`

#### Logging
- Currently None

___

### `format_time.py`
#### Purpose
Returns a date string in the agreed format.

#### Function
- `format_time`

#### Inputs
- `date_time` Mandatory, no default. a date time object to convert

#### Outputs
- `formatted_time` a string in the format YYYY-MM-DD HH:MM:SS.sss

#### Logging
- Currently None
___

### `generate_file_key.py`

#### Purpose
Generates file names based on table name, time, and file extension.

#### Function
- `generate_file_key`

#### Inputs
- `table_name` Mandatory, no default. String representing a database table name

- `end_time` Mandatory, no default. Datetime object representing the end time for the file name.

- `extension` Optional, default  "parquet". String representing the file extension. Omit the '.'

#### Logging
Currently None

___

### `generate_new_entry_query.py`

#### Purpose
Return a valid PostgreSQL query string for rows which were modified in the given table between the start_time and end_time. Will raise DateFormatError if start_time or end_time are in an invalid format.

#### Function
-  `generate_new_entry_query`

#### Inputs
- `table_name` Mandatory, no default. String representing the table name in the database.

- `start_time` Mandatory, no default. A `format_time` string representing the start of the time range (inclusive)

- `end_time` Mandatory, no default. A `format_time` string representing the end of the time range (exclusive)

#### Outputs
- `query` A string containing a SQL query for extracting from the database.

#### Logging
- Currently None

___

### `get_last_ingest_time.py`

#### Purpose
Inspect the ingested files for the time of the last successfully ingest.

#### Function
- `get_latest_filename`

#### Purpose
- Return the file in the bucket bucket_name with the prefix table_name which has the "biggest" name, ie. the name containing the latest timestamp.

#### Inputs
- `s3_client` Mandatory, no default.  boto3 s3 client connection object

- `bucket_name` Mandatory, no default. String contacting the name of the s3 bucket

- `table_name` Mandatory, no default. String containing the table name

#### Outputs
- `most_recent_filename` String, name of the most recent file for the tablename, bucket combination.

#### Logging
- Currently None

___

### `get_last_ingest_time.py`

#### Purpose
Return a datetime object corresponding to the key of the most-recently-modified file in bucket bucket_name with the prefix table_name.

#### Function
- `get_last_ingest_time`

#### Inputs
- `bucket_name` Mandatory, no default. String presenting the s3 bucket name

- `table_name` Mandatory, no default. String representing the db table name

#### Outputs
- `datetime` Returns a datetime object representing the latest extraction time for the bucket/table name combination

#### Logging
- Currently None

___

### `parquet_data.py`

#### Purpose
Takes dictionaries and converts them to bytes in parquet format

#### Function
- `parquet_data`

#### Purpose
- Takes a dictionary and turns it into a parquet formatted byte stream

#### Inputs
- `py_dict` Mandatory, no default. Pythin dictionary to be converted.

#### Outputs
- `out_buffer` BytesIO object containing the parquet data.

#### Logging
- Currently None

___

### `query_db.py`

#### Purpose
Take an sql query string and return the result of the query as a dictionary formatted like a json object with table names etc.

#### Function
-  `query_db`

#### Purpose
- Take an sql query string and return the result of the query as a dictionary formatted like a json object with table names etc.
    
- If `dict_name = ""`, only return the first row of the query response as a dictionary with column keys.
Otherwise, return a dictionary containing a list of dictionaries, where each dictionary contains a row of the query response.

#### Inputs
- `sql_string`: Mandatory, no default. String containing valid PostgreSQL query

- `connect_to_db`: Mandatory, no default. Function which returns connection to a database

- `close_db_connection` Mandatory, no default. Function which closes database connection

- `dict_name` Optional, default  "response" String name used in the key of the response dictionary

- `kwargs` Optional, no defaault. Keys and values passed into SQL query using :-syntax

#### Outputs
- `dict` dictionary containing response, depending on dict_name.

#### Logging
- Currently None
___

### `write_to_s3.py`

#### Purpose
Writes provided date to s3

#### Function
- `write_to_s3`

#### Inputs
- `s3_client` Mandatory, no default. Boto3.client("s3") object

- `bucket_name` Mandatory, no default. String containing the name of the bucket (must already exist)

- `file_key` Mandatory, no default, String,name of the file in the bucket, including directory and file extension

- `data` contents of the file`.

#### Outputs
- `None`

#### Logging
- Currently None

___

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

## Packages

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
___

## Load Process

## Overview

### `load.py`

The load process aims to:

- Copy parquet data from the s3 transform bucket
- Uploads the data to the warehouse database

#### Function
- `lambda_handler`

#### Purpose
- Sorts the tables so that the dim tables will be updated before the fact tables
- Generates queries for the corresponding tables that needs to be updated
- Uploads the data to the warehouse database

#### Inputs
- `event` Mandatory, no default. Takes the output from `transform.py` as the event
- `{"table_name_1": "path_to_dim/fact_table_1_in_tf_bucket",  "dim_counterparty": "pathtolatestdimcounterparty", "fact_payment": "path...", ...}`

- `context` Mandatory, no default. Metadata about the lambda handler.

#### Outputs
- `None`

#### Logging
- `CRITICAL` when each function fails fatally

## Packages

### `create_dim_query.py`
#### Purpose
Creates the query that will insert the updated values to the dim tables.

#### Function
- `create_dim_query`

#### Purpose
- Generates a query to delete all entries in a table and insert values from a parquet file.

#### Inputs
- `table_name` Mandatory, no default. Takes the table name of the file to be accessed from the s3 transform bucket.

- `table_path` Mandatory, no default. Takes the key of which file that needs to be accessed from the s3 transform bucket.

- `s3_client` Mandatory, no default. Boto3.client("s3") object

#### Outputs
- Returns a string of the query to be ran and appends/inserts values to the dim tables in the data warehouse.

#### Logging
- Currently None

___

#### Function
- `generate_insert_into_statement`

#### Purpose
- Appends `INSERT INTO` statement to the query with corresponding values from the DataFrame

#### Inputs
- `table_name` Mandatory, no default. Takes the name of the dim table that will be use to query the warehouse database.

- `columns` Mandatory, no default. Takes a list of names from the columns of the DataFrame.

- `df` Mandatory, no default. Takes the DataFrame of the corresponding dim table.

#### Outputs
- `output` The string query of the INSERT statement

#### Logging
- Currently None

___
#### Function
-  `format_value`

#### Purpose
- Checks if the value give is a `None` or `NULL` value and returns the string `"NULL"`. Otherwise it returns the string of the value.

#### Inputs
- `value` Mandatory, no default. Takes any value and formats it.

#### Outputs
- The string of the value or `"NULL"`

#### Logging
- Currently None
___

### `create_fact_query.py`

#### Purpose
Creates the query that will insert the updated values to the fact tables.

#### Function
- `create_fact_query`

#### Purpose
- Generates a query by fetching the DataFrame from the corresponding table name and key.

#### Inputs
- `table_name` Mandatory, no default. Takes the table name of the file to be accessed from the s3 transform bucket.

- `table_path` Mandatory, no default. Takes the key of which file that needs to be accessed from the s3 transform bucket.

- `s3_client` Mandatory, no default. Boto3.client("s3") object

#### Outputs
- Returns a string of the query to be ran and appends/inserts values to the fact tables in the data warehouse.

#### Logging
Currently None

___

### `generate_warehouse_query.py`
#### Purpose
Returns the database query string for updating the table table_name. 

#### Function
- `create_fact_query`

#### Purpose
- First it checks whether the table name is of a dim table or fact table then gets the correct type of query to return from the corresponding table.

#### Inputs
- `table_name` Mandatory, no default. Takes the table name accessed from the s3 transform bucket.

- `table_path` Mandatory, no default. Takes the key of which file that needs to be accessed from the s3 transform bucket.

- `s3_client` Mandatory, no default. Boto3.client("s3") object

#### Outputs
- Returns a string of the query to be ran and appends/inserts values to a tables in the data warehouse.

#### Logging
- Currently None
___

## Project Components - SQL

### `cleardb.sql`

#### Purpose
Makes sure that each time when changes are made and then deployed, all the data in each of the fact tables then dim tables are deleted. This is to make sure that the data inside the warehouse is accurate and up to date.

___

## Project Components - Terraform

## immutable-terraform
## Overview
The immutable-terraform is the process which allows for the initial set up of AWS. Allowing database credentials as well as warehouse credentials to be uploaded to AWS secretsmanager. Also, providing the creation of the ingest and transform buckets, in addition to the use of AWS Simple Notification Service (SNS).

### `main.tf`
#### Purpose
___

### `s3.tf`
#### Purpose
___
### `secrets.tf`
#### Purpose
___

### `ssm.tf`
#### Purpose
___

### `vars.tf`
#### Purpose
___


## main-terraform
## Overview

### `critical_email.tf`
#### Purpose
___

### `cw_log_group.tf`
#### Purpose
___

### `eventbridge.tf`
#### Purpose
___

### `iam.tf`
#### Purpose
___

### `lambda.tf`
#### Purpose
___

### `layers.tf`
#### Purpose
___

### `main.tf`
#### Purpose
___

### `s3.tf`
#### Purpose
___

### `secrets.tf`
#### Purpose
___

### `state_machine.tf`
#### Purpose
___

### `var.tf`
#### Purpose
___


## Project Components - Continuous Integration/Continuous Delivery (CI/CD)

