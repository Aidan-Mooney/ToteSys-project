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

## Project Components

### Python
#### Lambdas
##### ingest.py
##### transform.py
##### load.py

#### Utils
##### db_connections.py
##### format_time.py
##### generate_file_key.py
##### generate_new_entry_query.py
##### get_df_from_s3_parquet.py
##### parquet_data.py
##### warehouse.py
##### write_to_s3.py

### Terraform
#### immutable-terraform
##### main.tf
##### s3.tf
##### secrets.tf
##### ssm.tf
##### vars.tf

#### main-terraform
##### critical_email.tf
##### cw_log_group.tf
##### eventbridge.tf
##### iam.tf
##### lambda.tf
##### layers.tf
##### main.tf
##### s3.tf
##### secrets.tf
##### state_machine.tf
##### var.tf

### CICD
