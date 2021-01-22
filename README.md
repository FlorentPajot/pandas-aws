[![Build Status](https://travis-ci.com/FlorentPajot/pandas-aws.svg?branch=master)](https://travis-ci.com/FlorentPajot/pandas-aws) [![codecov](https://codecov.io/gh/FlorentPajot/pandas-aws/branch/master/graph/badge.svg)](https://codecov.io/gh/FlorentPajot/pandas-aws)

# Pandas AWS - AWS use made easy for data scientists

Pandas AWS makes it super easy to use a pandas.DataFrame along with AWS services.

## Working with S3

First create an S3 client to be used later and define a bucket
```
from pandas_aws import get_client

s3 = get_client('s3')
MY_BUCKET= 'pandas-aws-bucket'
```
Example 1: get a DataFrame from a parquet file stored in S3
```
from pandas_aws.s3 import get_df

df_from_parquet_file = get_df(s3, MY_BUCKET, 'my_parquet_file_path', format='parquet')
```
Example 2: get a DataFrame from multiple CSV files stored in S3
```
from pandas_aws.s3 import get_df_from_keys

df_from_list = get_df_from_keys(s3, MY_BUCKET, prefix='my-folder', suffix='.csv')
```
Example 3: put a DataFrame into S3 using an xlsx (Excel) file format
```
from pandas_aws.s3 import put_df

put_df(s3, my_dataframe, MY_BUCKET, 'target_file_path', format='xlsx')
```

## Working with Redshift
First create a RedshiftClient object (boto3 doesn't provide a redshift client for executing requests)
```
from sqlalchemy import create_engine
from pandas_aws importt get_client
from pandas_aws.redshift import RedshiftClient

s3 = get_client('s3')
postgres_engine = create_engine('MY_REDSHIFT_ENPOINT')

redshift = RedshiftClient(postgres_engine, 'my_redshift_schema', s3_client=s3)
```
Example 1: load a DataFrame content into a Redshift table
```
# In case the target table doesn't exists, the table is created
# based on the DataFrame schema and content
redshift.upload_to_redshift(my_dataframe,
                            'target_table_name',
                            MY_BUKET,
                            'temp_file_path',
                            aws_role='role-for-redshift-s3-access-arn'
                            )

```
Example 2: upsert (update existing rows, insert new ones) data from a DataFrame into a Redshift table
```
redshif.upsert_rows(
                    my_dataframe,
                    'target_table_name',
                    MY_BUKET,
                    'temp_file_path',
                    comparison_key = ['table_pk']
                    comparison_key: list,
                    aws_role='role-for-redshift-s3-access-arn'
                    )
```

# Installing pandas-aws

## Pip installation

You can use pip to download the package

`pip install pandas-aws`

# Contributing to pandas-aws

## Git clone

We use the `develop` brand as the release branch, thus `git clone` the repository and `git checkout develop` in order to get the latest version in development.

```
git clone git@github.com:FlorentPajot/pandas-aws.git
```

## Preparing your environment

Pandas AWS uses `poetry` to manage dependencies. Thus, `poetry` is required:

`curl -SSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python`

Create a separate Python environment, for example using `pyenv`:

```
pyenv virtualenv pandas-aws
pyenv activate pandas-aws
```
Then install dependencies with poetry after your `git clone` from the project repository:

`poetry install`

## Guidelines

Todo
