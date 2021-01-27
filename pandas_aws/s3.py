#  -*- coding: utf-8 -*-
__author__ = 'fpajot'

import gzip
from io import StringIO, BytesIO
import logging
import pickle

import boto3
import pandas

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


def put_df(s3: boto3.resources.base.ServiceResource,
           df: pandas.DataFrame,
           bucket: str,
           key: str,
           format: str = 'csv',
           compression: str = None,
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
    :param '**kwargs': used for passing arguments to pandas writing methods
    """
    # Uploads the given file using a managed uploader,
    # which will split up large files automatically
    # and upload parts in parallel

    assert format in ['csv', 'parquet', 'pickle', 'xlsx'], \
        'provider format value not accepted'
    if format == 'csv':
        assert compression in [None, 'gzip'], \
            'provider compression value not accepted'

    if isinstance(df, pandas.DataFrame):
        if format == 'csv':
            buffer = StringIO()
            df.to_csv(buffer, index_label=False, index=False, **kwargs)
            if compression == 'gzip':
                logger.info('Using csv compression with gzip')
                gz_buffer = BytesIO()
                buffer.seek(0)
                # compress string stream using gzip
                with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
                    gz_file.write(bytes(buffer.getvalue(), 'utf-8'))
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    ContentType='text/csv',  # the original type
                    ContentEncoding='gzip',  # MUST have or browsers will error
                    Body=gz_buffer.getvalue()
                )
                logger.info(f'File uploaded using format {format}, \
                            compression {compression}')
                return
            else:
                body = buffer.getvalue()
        elif format == 'xlsx':
            buffer = BytesIO()
            writer = pandas.ExcelWriter(buffer, engine='xlsxwriter')
            df.to_excel(writer, sheet_name='Sheet1', index=False, **kwargs)
            writer.save()
            body = buffer.getvalue()
        elif format == 'parquet':
            if 'engine' in kwargs:
                engine = kwargs['engine']
            else:
                engine = 'pyarrow'
            buffer = BytesIO()
            df.to_parquet(buffer, engine=engine, **kwargs)
            body = buffer.getvalue()
        elif format == 'pickle':
            body = pickle.dumps(df)
        else:
            raise TypeError('File type not supported')
        s3.put_object(Body=body, Bucket=bucket, Key=key)
        logger.info(f'File uploaded using format {format}, \
                    compression {compression}')
    else:
        raise TypeError('Provided content must type pandas.DataFrame')


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
                     format: str = 'suffix',
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
