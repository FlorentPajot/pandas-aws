[![Build Status](https://travis-ci.com/FlorentPajot/pandas-aws.svg?branch=master)](https://travis-ci.com/FlorentPajot/pandas-aws) [![codecov](https://codecov.io/gh/FlorentPajot/pandas-aws/branch/master/graph/badge.svg)](https://codecov.io/gh/FlorentPajot/pandas-aws)

# Pandas AWS - AWS use made easy for data scientists

Pandas AWS makes it super easy to use a pandas.DataFrame along with AWS services.

```
# Example : get a DataFrame from multiple CSV files in S3

from pandas_aws import get_client, get_df_from_keys

MY_BUCKET= 'pandas-aws-bucket'

s3 = get_client('s3')

df = get_df_from_keys(s3, MY_BUCKET, prefix='my-folder', suffix='.csv')
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
