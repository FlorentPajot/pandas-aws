#  -*- coding: utf-8 -*-
__author__ = 'fpajot'

import io
import logging
import pickle
from unittest import TestCase

import boto3
from botocore.exceptions import ClientError
from moto import mock_s3
import pandas

from pandas_aws.redshift import RedshiftClient

MY_BUCKET = "mymockbucket"
MY_PREFIX = "mockfolder"
AWS_REGION_NAME = 'eu-west-1'

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@mock_s3
class BaseAWSTest(TestCase):
    """Base class for test cases using moto"""

    def setUp(self):
        self.data = {'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']}
        self.client = boto3.client("s3", region_name=AWS_REGION_NAME)
        s3 = boto3.resource("s3", region_name=AWS_REGION_NAME)
        if MY_BUCKET in [b for b in s3.buckets.all()]:
            err = "{bucket} should not exist.".format(bucket=MY_BUCKET)
            logger.error([b['Name'] for b in self.client.list_buckets()['Buckets']])
            raise EnvironmentError(err)
        else:
            self.client.create_bucket(Bucket=MY_BUCKET, CreateBucketConfiguration={
                'LocationConstraint': AWS_REGION_NAME})
            logger.debug("Existing buckets:")
            logger.debug(self.client.list_buckets()['Buckets'])

    def tearDown(self):
        s3 = boto3.resource("s3", region_name=AWS_REGION_NAME)
        bucket = s3.Bucket(MY_BUCKET)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()
