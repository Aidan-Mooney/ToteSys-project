# Terrific Totes 

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

## Package

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

### `cleardb.sql`
#### Purpose
Makes sure that each time when changes are made and then deployed, all the data in each of the fact tables then dim tables are deleted. This is to make sure that the data inside the warehouse is accurate and up to date.