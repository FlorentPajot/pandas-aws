__version__ = '0.1.0'
__author__ = 'fpajot'

import os


from moto import mock_s3
import pytest

@pytest.fixture(scope='session')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-1'
    os.environ['AWS_PROFILE'] = 'default'


@pytest.fixture(scope='session')
def s3(aws_credentials):
    from aws_pocket import get_client

    with mock_s3():
        yield get_client('s3')
