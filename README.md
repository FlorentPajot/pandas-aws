[![Build Status](https://travis-ci.com/FlorentPajot/pandas-aws.svg?branch=master)](https://travis-ci.com/FlorentPajot/pandas-aws) [![codecov](https://codecov.io/gh/FlorentPajot/pandas-aws/branch/master/graph/badge.svg)](https://codecov.io/gh/FlorentPajot/pandas-aws)

# Pandas AWS - AWS made easy for data scientists

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
Example 2: get a DataFrame from multiple CSV files (with same schema) stored in S3
```
from pandas_aws.s3 import get_df_from_keys

df_from_list = get_df_from_keys(s3, MY_BUCKET, prefix='my-folder', suffix='.csv')
```
Example 3: put a DataFrame into S3 using an xlsx (Excel) file format
```
from pandas_aws.s3 import put_df

put_df(s3, my_dataframe, MY_BUCKET, 'target_file_path', format='xlsx')
```
Example 4: put a DataFrame into S3 using multi parts upload
```
from pandas_aws.s3 import put_df

put_df(s3, my_dataframe, MY_BUCKET, 'target_file_path', format='csv', compression='gzip', parts=8)
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

Create a separate Python environment, for example using `pyenv` along with `pyenv-virtualenv` and Python 3.7.7:

```
pyenv install 3.7.7
pyenv virtualenv 3.7.7 pandas-aws
pyenv activate pandas-aws
```

Check your environment using:
```
which python
// should show something like .pyenv/shims/python
python -V
// should show python 3.7.7 (or any other version you selected)
pip list
// should show barely nothing except pip and setuptools
```

In cas your encounter a problem, check `Pyenv` documentation.

Then install dependencies with poetry after your `git clone` from the project repository:

`poetry install`

## Guidelines

Todo

## Requires
The project needs the following dependencies:
- libpq-dev (psycopg2 dependency)
