# Terrific Totes

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
- `event` Mandatory, no default. Structure of event: `{"tables_to_query": ["table_name",...]}`

- `context` Mandatory, no default. Metadata about the lambda handler.

#### Outputs
- A dictionary containing the table names as keys and file_key as value.
-  `{
            'table_name_1' : 'table_name/yyyy/mm/dd/hhmmssmmmmmm.parquet',
        'table_name_2' : 'table_name/yyyy/mm/dd/hhmmssmmmmmm.parquet',
            ...
        }`

#### Logging
- `INFO` when each function is successful
- `CRITICAL` when each function fails fatally
- `WARNING` when query_db returns an empty list

## Package

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