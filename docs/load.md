# Terrific Totes 

## Transform Process

## Overview

The load process aims to:

- Read the parquet files from the [transform process](transform.md)
- Loads each data from the dim tables to the data warehouse first
- Then loads each data from the fact tables to the data warehouse

## Package

### `create_dim_query.py`
#### Purpose
Creates the query that will insert the updated values to the dim tables.

#### Functions
##### `create_dim_query`

###### Inputs
`table_name` Mandatory, no default. 

`table_path` Mandatory, no default. 

`s3_client` Mandatory, no default.

###### Outputs
Returns a string of the query to be ran and update values to the dim tables in the data warehouse.

###### Logging
Currently None

___

##### `generate_insert_into_statement`
#### Purpose
Creates the query that will insert the updated values to the dim tables.

#### Functions
##### `create_dim_query`

###### Inputs
`table_name` Mandatory, no default. 

`table_path` Mandatory, no default. 

`s3_client` Mandatory, no default.

`data` contents of the file`.

###### Outputs


###### Logging
Currently None

___





### `create_fact_query.py`

### `generate_warehouse_query.py`

### `cleardb.sql`