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

from pandas_aws.s3 import get_keys, put_df, get_df, get_df_from_keys

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


class GetKeysTests(BaseAWSTest):
    """Test s3.get_keys"""

    def setUp(self):
        super(GetKeysTests, self).setUp()
        for o in range(2):
            self.client.put_object(Bucket=MY_BUCKET, Key=MY_PREFIX + '/key' + str(o), Body=str(o))

    def tearDown(self):
        super(GetKeysTests, self).tearDown()

    def test_get_s3_keys_success_one_key(self):
        key = next(get_keys(self.client, MY_BUCKET, MaxKeys=1))
        self.assertEqual(key, MY_PREFIX + '/key0')

    def test_get_s3_keys_failure_one_key(self):
        with self.assertRaises(StopIteration):
            _ = next(get_keys(self.client, MY_BUCKET, prefix='foo'))

    def test_get_s3_keys_success_multi_pages(self):

        keys = get_keys(self.client, MY_BUCKET, MaxKeys=1)
        self.assertEqual(next(keys), MY_PREFIX + '/key0')
        self.assertEqual(next(keys), MY_PREFIX + '/key1')

    def test_get_s3_keys_failure_multi_pages(self):
        keys = get_keys(self.client, MY_BUCKET)
        _ = next(keys)
        _ = next(keys)
        with self.assertRaises(StopIteration):
            _ = next(keys)

    def test_get_s3_keys_success_one_key_with_prefix(self):
        self.client.put_object(Bucket=MY_BUCKET, Key='key3', Body='awesome body')
        keys = list(get_keys(self.client, MY_BUCKET, prefix=MY_PREFIX))
        self.assertEqual(keys, [MY_PREFIX + '/key0', MY_PREFIX + '/key1'])

    def test_get_s3_keys_success_one_key_with_suffix(self):
        self.client.put_object(Bucket=MY_BUCKET, Key='key3.txt', Body='awesome body')
        key = next(get_keys(self.client, MY_BUCKET, suffix='.txt'))
        self.assertEqual(key, 'key3.txt')


class PutDFTests(BaseAWSTest):
    """Test for s3.put_df"""

    def setUp(self):
        super(PutDFTests, self).setUp()

    def tearDown(self):
        super(PutDFTests, self).tearDown()

    def test_put_df_failure_unknown_type(self):
        o = 'awesome body'
        key = MY_PREFIX + '/key1'
        with self.assertRaises(TypeError):
            put_df(self.client, o, MY_BUCKET, key)

    def test_put_df_success_dataframe_to_pickle(self):
        o = pandas.DataFrame.from_dict(self.data)
        key = MY_PREFIX + '/key1.pickle'
        put_df(self.client, o, MY_BUCKET, key, format='pickle')
        body = pickle.loads(self.client.get_object(Bucket=MY_BUCKET, Key=key)['Body'].read())
        self.assertSequenceEqual(list(o.columns), list(body.columns))
        self.assertSequenceEqual(o.iloc[0].tolist(), body.iloc[0].tolist())

    def test_put_df_success_dataframe_to_csv(self):
        o = pandas.DataFrame.from_dict(self.data)
        key = MY_PREFIX + '/key1.csv'
        put_df(self.client, o, MY_BUCKET, key, format='csv')
        body = pandas.read_csv(self.client.get_object(Bucket=MY_BUCKET, Key=key)['Body'])
        self.assertSequenceEqual(list(o.columns), list(body.columns))
        self.assertSequenceEqual(o.iloc[0].tolist(), body.iloc[0].tolist())

    def test_put_df_success_dataframe_to_parquet(self):
        o = pandas.DataFrame.from_dict(self.data)
        key = MY_PREFIX + '/key1.parquet'
        put_df(self.client, o, MY_BUCKET, key, format='parquet')
        body = pandas.read_parquet(io.BytesIO(self.client.get_object(Bucket=MY_BUCKET, Key=key)['Body'].read()))
        self.assertSequenceEqual(list(o.columns), list(body.columns))
        self.assertSequenceEqual(o.iloc[0].tolist(), body.iloc[0].tolist())

    def test_put_df_success_dataframe_to_excel(self):
        o = pandas.DataFrame.from_dict(self.data)
        key = MY_PREFIX + '/key1.xlsx'
        put_df(self.client, o, MY_BUCKET, key, format='xlsx')
        body = pandas.read_excel(io.BytesIO(self.client.get_object(Bucket=MY_BUCKET, Key=key)['Body'].read()))
        self.assertSequenceEqual(list(o.columns), list(body.columns))
        self.assertSequenceEqual(o.iloc[0].tolist(), body.iloc[0].tolist())

    def test_put_df_failure_dataframe_to_unknown_format(self):
        o = pandas.DataFrame.from_dict(self.data)
        key = MY_PREFIX + '/key1.txt'
        with self.assertRaises(AssertionError):
            put_df(self.client, o, MY_BUCKET, key, format='txt')

    def test_put_df_success_dataframe_to_csv_with_kwargs(self):
        o = pandas.DataFrame.from_dict(self.data)
        key = MY_PREFIX + '/key1.csv'
        put_df(self.client, o, MY_BUCKET, key, format='csv', sep=';')
        body = pandas.read_csv(self.client.get_object(Bucket=MY_BUCKET, Key=key)['Body'], sep=';')
        self.assertSequenceEqual(list(o.columns), list(body.columns))
        self.assertSequenceEqual(o.iloc[0].tolist(), body.iloc[0].tolist())


class GetDFTests(BaseAWSTest):
    """Test for s3.get_df"""

    def setUp(self):
        super(GetDFTests, self).setUp()

    def tearDown(self):
        super(GetDFTests, self).tearDown()

    def test_get_df_success_unknown_type(self):
        with self.assertRaises(AssertionError):
            _ = get_df(self.client, MY_BUCKET, MY_PREFIX + '/key', format='txt')

    def test_get_df_failure(self):
        self.client.put_object(Bucket=MY_BUCKET, Key=MY_PREFIX + '/key1', Body='awesome body')
        with self.assertRaises(ClientError):
            _ = get_df(self.client, MY_BUCKET, 'unknown_key', format='pickle')

    def test_get_df_success_with_pickle_type(self):
        o = pandas.DataFrame.from_dict(self.data)
        self.client.put_object(Body=pickle.dumps(o), Bucket=MY_BUCKET, Key=MY_PREFIX + '/key1.pickle')
        s3_o = get_df(self.client, MY_BUCKET, MY_PREFIX + '/key1.pickle', format='pickle')
        self.assertSequenceEqual(list(o.columns), list(s3_o.columns))
        self.assertSequenceEqual(o.iloc[0].tolist(), s3_o.iloc[0].tolist())

    def test_get_df_success_with_csv_type(self):
        buffer = io.StringIO()
        df = pandas.DataFrame.from_dict(self.data)
        df.to_csv(buffer, index_label=False)
        self.client.put_object(Bucket=MY_BUCKET, Key=MY_PREFIX + '/key1.csv', Body=buffer.getvalue())
        o = get_df(self.client, MY_BUCKET, MY_PREFIX + '/key1.csv', format='csv')
        self.assertSequenceEqual(list(o.columns), list(df.columns))
        self.assertSequenceEqual(o.iloc[0].tolist(), df.iloc[0].tolist())

    def test_get_df_success_with_xlsx_type(self):
        buffer = io.BytesIO()
        df = pandas.DataFrame.from_dict(self.data)
        writer = pandas.ExcelWriter(buffer, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        writer.save()
        self.client.put_object(Bucket=MY_BUCKET, Key=MY_PREFIX + '/key1.xlsx', Body=buffer.getvalue())
        o = get_df(self.client, MY_BUCKET, MY_PREFIX + '/key1.xlsx', format='xlsx')
        self.assertSequenceEqual(list(o.columns), list(df.columns))
        self.assertSequenceEqual(o.iloc[0].tolist(), df.iloc[0].tolist())

    def test_get_df_success_with_parquet_type(self):
        buffer = io.BytesIO()
        df = pandas.DataFrame.from_dict(self.data)
        df.to_parquet(buffer, engine='pyarrow')
        self.client.put_object(Bucket=MY_BUCKET, Key=MY_PREFIX + '/key1.parquet', Body=buffer.getvalue())
        o = get_df(self.client, MY_BUCKET, MY_PREFIX + '/key1.parquet', format='parquet')
        self.assertSequenceEqual(list(o.columns), list(df.columns))
        self.assertSequenceEqual(o.iloc[0].tolist(), df.iloc[0].tolist())


class GetDFFromKeysTests(BaseAWSTest):
    """Test for s3.get_df_from_keys"""

    def setUp(self):
        super(GetDFFromKeysTests, self).setUp()
        df = pandas.DataFrame.from_dict(self.data)
        # upload different files to our test bucket
        for format in ["csv", "xlsx", "parquet"]:
            for file in [f"/key1.{format}", f"/key2.{format}"]:
                if format == "csv":
                    buffer = io.StringIO()
                    df.to_csv(buffer, index_label=False)
                elif format == "xlsx":
                    buffer = io.BytesIO()
                    writer = pandas.ExcelWriter(buffer, engine='xlsxwriter')
                    df.to_excel(writer, sheet_name='Sheet1', index=False)
                    writer.save()
                elif format == "parquet":
                    buffer = io.BytesIO()
                    df.to_parquet(buffer, engine='pyarrow')
                self.client.put_object(Bucket=MY_BUCKET, Key=MY_PREFIX + file, Body=buffer.getvalue())

    def tearDown(self):
        super(GetDFFromKeysTests, self).tearDown()

    def test_get_df_from_multiple_files(self):
        def check(df, n_files=2):
            self.assertSequenceEqual(list(self.data.keys()), list(df.columns))
            self.assertEqual(len(self.data['col_1']) * n_files, df.shape[0])
            self.assertEqual(sum(self.data['col_1']) * n_files, df.loc[:, 'col_1'].sum())

        # check 'suffix' format option
        for suffix in ['.csv', '.xlsx', '.parquet']:
            # 2 files per extension
            df = get_df_from_keys(self.client,
                                  MY_BUCKET,
                                  MY_PREFIX,
                                  suffix=suffix,
                                  format='suffix')
            check(df, n_files=2)

        # check 'mixed' format option
        df = get_df_from_keys(self.client, MY_BUCKET, MY_PREFIX, format='mixed')
        check(df, n_files=6)

        # check valid format option like 'csv'
        df = get_df_from_keys(self.client,
                              MY_BUCKET,
                              MY_PREFIX,
                              suffix='.csv',
                              format='csv')
        check(df, n_files=2)

        # check invalid format option
        with self.assertRaises(AssertionError):
            _ = get_df_from_keys(self.client,
                                  MY_BUCKET,
                                  MY_PREFIX,
                                  suffix='.csv',
                                  format='invalid_value')

        # check no data
        df = get_df_from_keys(self.client, MY_BUCKET, MY_PREFIX, suffix='egs')
        self.assertEqual(df, None)
