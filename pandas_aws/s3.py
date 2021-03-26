#  -*- coding: utf-8 -*-
__author__ = 'fpajot'

import gzip
from io import StringIO, BytesIO
import logging
from os import path
import pickle

import boto3
import pandas
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_keys(s3: boto3.resources.base.ServiceResource,
             bucket: str, prefix: str = '',
             suffix: str = '',
             **kwargs):
    """
    Generate the keys in an S3 bucket.
    :param s3: S3 client
    :param bucket: S3 bucket name.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    :param '**kwargs': used for passing arguments to list_objects_v2 method
    """

    kwargs.update({'Bucket': bucket})

    # do the filtering directly with S3 API.
    if isinstance(prefix, str):
        kwargs.update({'Prefix': prefix})

    done = False
    while not done:
        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        if 'Contents' in resp.keys():
            for obj in resp['Contents']:
                key = obj['Key']
                if key.endswith(suffix):
                    yield key
        else:
            logger.info('Nothing found for the given prefix and/or suffix')

        # The S3 API is paginated, default MaxKeys is 123
        done = not resp['IsTruncated']
        if not done:
            kwargs.update({'ContinuationToken': resp['NextContinuationToken']})


def _get_splited_df_streams(df, parts, func, buffer_class, **kwargs):
    """
    Splits pandas.Dataframe into parts and returns list of correspond streams objects
    :param df: pandas Dataframe which to be splitted
    :param parts: number of output files
    :param func: function to dump Dataframe
    :param buffer_class: class of stream I/O
    :param sort_keys: list of column names (sort keys)
    :param '**kwargs': used for passing arguments to dumping Dataframe functions
    :return: list of streams, contain parts of Dataframe
    :rtype: list
    """
    if 'sort_keys' in kwargs.keys():
        sort_keys = kwargs['sort_keys']
        del kwargs['sort_keys']
    else:
        sort_keys = None

    if sort_keys is not None:
        assert len(sort_keys) > 0, 'Sort keys not accepted, it must be not empty list of strings'

    func_kwargs = {}
    for key in kwargs.keys():
        if key in list(func.__code__.co_varnames):
            func_kwargs[key] = kwargs[key]

    buffers = []

    if sort_keys is None:
        parts_df = np.array_split(df, parts)
    else:
        parts_df = np.array_split(df.sort_values(sort_keys), parts)
    for p in parts_df:
        b = buffer_class()
        if func == pandas.DataFrame.to_excel:
            w = pandas.ExcelWriter(b, engine='xlsxwriter')
            func(p, w, **func_kwargs)
            w.save()
        else:
            func(p, b, **func_kwargs)
        buffers.append(b)
    return buffers


def put_df(s3: boto3.resources.base.ServiceResource,
           df: pandas.DataFrame,
           bucket: str,
           key: str,
           **kwargs
           ):
    """
    Put pandas.DataFrame object to s3 using a specific format
    :param s3: S3 client
    :param df: DataFrame to put into s3
    :param bucket: bucket name of the target file
    :param key: aws key of the target file
    :param format: file format to use, i.e csv
    :param compression: file compression applied
    :param parts: number of output files
    :param sort_keys: list of column names (sort keys)
    :param '**kwargs': used for passing arguments to pandas writing methods
    """
    # Uploads the given file using a managed uploader,
    # which will split up large files automatically
    # and upload parts in parallel
    if not isinstance(df, pandas.DataFrame):
        raise TypeError('Provided content must type pandas.DataFrame')

    if 'format' in kwargs.keys():
        format = kwargs['format']
        del kwargs['format']
    else:
        format = 'csv'

    if 'compression' in kwargs.keys():
        compression = kwargs['compression']
        del kwargs['compression']
    else:
        compression = None

    if 'parts' in kwargs.keys():
        parts = kwargs['parts']
        del kwargs['parts']
    else:
        parts = 1

    assert parts > 0, 'Number of parts not accepted, it must be > 0'

    assert format in ['csv', 'parquet', 'pickle', 'xlsx'], \
        'provider format value not accepted'

    if format == 'csv':
        assert compression in [None, 'gzip'], \
            'provider compression value not accepted'

    buffers = []
    content_type = 'text'
    content_encoding = 'default'

    if format == 'csv':
        kwargs['index_label'] = False
        kwargs['index'] = False
        buffers = _get_splited_df_streams(df, parts, pandas.DataFrame.to_csv, StringIO, **kwargs)
        if compression == 'gzip':
            logger.info('Using csv compression with gzip')
            content_type = 'text/csv'  # the original type
            content_encoding = 'gzip'  # MUST have or browsers will error
            tmp_buffer = []
            for buffer in buffers:
                buffer.seek(0)
                gz_buffer = BytesIO()
                # compress string stream using gzip
                with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
                    gz_file.write(bytes(buffer.getvalue(), 'utf-8'))
                tmp_buffer.append(gz_buffer)
            buffers = tmp_buffer
    elif format == 'xlsx':
        kwargs['sheet_name'] = 'Sheet1'
        kwargs['index'] = False
        buffers = _get_splited_df_streams(df, parts, pandas.DataFrame.to_excel, BytesIO, **kwargs)
    elif format == 'parquet':
        if 'engine' in kwargs:
            engine = kwargs['engine']
        else:
            engine = 'pyarrow'
        buffers = _get_splited_df_streams(df, parts, pandas.DataFrame.to_parquet, BytesIO, engine=engine, **kwargs)
    elif format == 'pickle':
        buffers = _get_splited_df_streams(df, parts, pickle.dump, BytesIO)
        content_encoding = 'application/octet-stream'
    else:
        raise TypeError('File type not supported')

    for bid, buffer in enumerate(buffers, start=1):
        if parts == 1:
            key_str = key
        else:
            dirname, basename = path.split(key)
            basename_parts = basename.split(sep='.')
            obj_name = '.'.join([basename_parts[0], str(bid)] + basename_parts[1:])
            key_str = '/'.join([dirname, basename_parts[0], obj_name])
        s3.put_object(
                    Bucket=bucket,
                    Key=key_str,
                    ContentType=content_type,  # the original type
                    ContentEncoding=content_encoding,  # MUST have or browsers will error
                    Body=buffer.getvalue()
                )

    if compression is None:
        logger.info(f'File uploaded using format {format}')
    else:
        logger.info(f'File uploaded using format {format}, '
                    f'compression {compression}')


def get_df(s3: boto3.resources.base.ServiceResource,
           bucket: str,
           key: str,
           format: str,
           **kwargs):
    """
    Import object from s3 and convert to pandas_utils.DataFrame if possible
    :param s3: S3 client
    :param bucket: bucket name of the target file
    :param key: aws key of the target file
    :param format: file format to get DataFrame from, i.e csv
    :param compression: file compression used
    :param '**kwargs': used for passing arguments to pandas reading methods
    :return: DataFrame from data in S3
    :rtype: pandas.DataFrame
    """

    assert format in ['csv', 'parquet', 'pickle', 'xlsx'], \
        'provider format value not accepted'

    object_ = s3.get_object(Bucket=bucket, Key=key)

    if format == 'pickle':
        return pickle.loads(object_['Body'].read(), **kwargs)
    elif format == 'csv':
        return pandas.read_csv(object_['Body'], **kwargs)
    elif format == 'parquet':
        return pandas.read_parquet(BytesIO(object_['Body'].read()), **kwargs)
    elif format == 'xlsx':
        return pandas.read_excel(BytesIO(object_['Body'].read()), **kwargs)


def get_df_from_keys(s3: boto3.resources.base.ServiceResource,
                     bucket: str,
                     prefix: str,
                     suffix: str = '',
                     **kwargs):
    """
    Build a DataFrame from multiple files in the same folder in S3
    :param s3: S3 client
    :param bucket: bucket name of the target file
    :param prefix: aws key of the target file
    :param suffix: suffix to match when looking for files
    :param format: file format to get DataFrame from, i.e csv
    :rtype: pandas.DataFrame
    """

    if 'format' in kwargs.keys():
        format = kwargs['format']
        del kwargs['format']
    else:
        format = 'suffix'
    assert format in ["csv", "parquet", "xlsx", "suffix", "mixed"], f"{format} format not supported"
    if format == "mixed":
        logger.warning('Mixed format used, might discard files')

    l_df = list()
    for f in get_keys(s3, bucket, prefix=prefix, suffix=suffix):
        if f != prefix:
            if format == 'suffix':
                logger.warning('Auto format detection based on suffix used')
                format = f.split('.')[-1]
                obj_ = get_df(s3, bucket, f, format, **kwargs)
                l_df.append(obj_)
            elif format == 'mixed':
                processed = False
                for format_ in ['csv', 'parquet', 'xlsx']:
                    try:
                        obj_ = get_df(s3, bucket, f, format_, **kwargs)
                        l_df.append(obj_)
                        processed = True
                    except Exception:
                        pass
                if processed == False:
                    logger.warning(f'No format matched for file {f}')
            else:
                obj_ = get_df(s3, bucket, f, format, **kwargs)
                l_df.append(obj_)

    if len(l_df) > 0:
        return pandas.concat(l_df, axis=0, ignore_index=True) \
                     .reset_index(drop=True)
    else:
        return None
