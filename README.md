# ToteSys Data Engineering Project

## Introduction

## Table of Content
Usage 
Set-up development environment
Deployment
Project Components
Python
Terraform
CICD

## Usage

### Set up development environment
- database creds
- email address
- export AWS creds into the environment
- requirements
- how to run the make commands

### Deployment
- run immutable-terraform first
- add aws access key, region, secret access key, db creds, saftey api key to github secrets
- then main-terraform

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
