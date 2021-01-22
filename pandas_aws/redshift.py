dfimport datetime
import logging
import os
import traceback
import sys

import botocore
import psycopg2
from pandas import DataFrame

from . import get_client
from .s3 import put_df

logger = logging.getLogger()

class RedshiftClient(object):

    def __init__(
                self,
                pg_connector,
                schema: str,
                s3_client: boto3.resources.base.ServiceResource = None,
                profile_name: str = 'default',
                **kwargs
                ):

        self.schema = schema
        self.profile_name = profile_name
        self.connector = pg_connector

        if s3_client is not None:
            if isinstance(s3_client, boto3.resources.base.ServiceResource):
                self.s3_client = s3_client
            else:
                raise TypeError("expected s3_client of type botocore.client.S3")
        else:
            self.s3_client = get_client('s3', profile_name=self.profile_name)

        logger.info("Connected to Redshift")

        self.cursor = self.connector.cursor()
        self._reserved_words = ['AES128', 'AES256', 'ALL', 'ALLOWOVERWRITE',
                                'ANALYSE', 'ANALYZE', 'AND', 'ANY', 'ARRAY',
                                'AS', 'ASC', 'AUTHORIZATION', 'BACKUP', 'BETWEEN',
                                'BINARY', 'BLANKSASNULL', 'BOTH', 'BYTEDICT', 'BZIP2',
                                'CASE', 'CAST', 'CHECK', 'COLLATE', 'COLUMN', 'CONSTRAINT',
                                'CREATE', 'CREDENTIALS', 'CROSS', 'CURRENT_DATE',
                                'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'CURRENT_USER',
                                'CURRENT_USER_ID', 'DEFAULT', 'DEFERRABLE', 'DEFLATE',
                                'DEFRAG', 'DELTA', 'DELTA32K', 'DESC', 'DISABLE',
                                'DISTINCT', 'DO', 'ELSE', 'EMPTYASNULL', 'ENABLE',
                                'ENCODE', 'ENCRYPT', 'ENCRYPTION', 'END', 'EXCEPT',
                                'EXPLICIT', 'FALSE', 'FOR', 'FOREIGN', 'FREEZE', 'FROM',
                                'FULL', 'GLOBALDICT256', 'GLOBALDICT64K', 'GRANT', 'GROUP',
                                'GZIP', 'HAVING', 'IDENTITY', 'IGNORE', 'ILIKE', 'IN',
                                'INITIALLY', 'INNER', 'INTERSECT', 'INTO', 'IS', 'ISNULL',
                                'JOIN', 'LEADING', 'LEFT', 'LIKE', 'LIMIT', 'LOCALTIME',
                                'LOCALTIMESTAMP', 'LUN', 'LUNS', 'LZO', 'LZOP', 'MINUS',
                                'MOSTLY13', 'MOSTLY32', 'MOSTLY8', 'NATURAL', 'NEW', 'NOT',
                                'NOTNULL', 'NULL', 'NULLS', 'OFF', 'OFFLINE', 'OFFSET',
                                'OID', 'OLD', 'ON', 'ONLY', 'OPEN', 'OR', 'ORDER', 'OUTER',
                                'OVERLAPS', 'PARALLEL', 'PARTITION', 'PERCENT', 'PERMISSIONS',
                                'PLACING', 'PRIMARY', 'RAW', 'READRATIO', 'RECOVER',
                                'REFERENCES', 'RESPECT', 'REJECTLOG', 'RESORT', 'RESTORE',
                                'RIGHT', 'SELECT', 'SESSION_USER', 'SIMILAR', 'SNAPSHOT ',
                                'SOME', 'SYSDATE', 'SYSTEM', 'TABLE', 'TAG', 'TDES',
                                'TEXT255', 'TEXT32K', 'THEN', 'TIMESTAMP', 'TO', 'TOP',
                                'TRAILING', 'TRUE', 'TRUNCATECOLUMNS', 'UNION', 'UNIQUE',
                                'USER', 'USING', 'VERBOSE', 'WALLET', 'WHEN', 'WHERE',
                                'WITH', 'WITHOUT']

    def __enter__(self, **kwargs):
        return self

    def __exit__(self, *args):
        self.cursor.close()

    def add_reserved_words(self, words: list) -> None:
        """Adds a reserved word to the connector attribute for later use"""

        if isinstance(words, list):
                self._reserved_words = self._reserved_words + \
                [str(w) for w in set(words) for w not in self._reserved_words]
        else:
            raise TypeError(f'Invalid type passed to add_reserved_words(): {type(words)}, expected str of list ')

    def _validate_column_names(self, df: DataFrame) -> DataFrame:
        """Validate the column names to ensure no reserved words are used."""

        reserved_words = [r.strip().lower() for r in self._reserved_words]

        df.columns = [x.lower().replace(' ', '_') for x in df.columns]

        for col in df.columns:
            try:
                assert col not in reserved_words
            except AssertionError:
                raise ValueError(f'DataFrame column name {col} is a reserved word')
        return df

    @staticmethod
    def _to_redshift_types(dtype_: str) -> str:
        """Retrieves corresponding valid Redshift type given a type name"""
        if dtype_.startswith('int'):
            return 'INTEGER'
        elif dtype_.startswith('float'):
            return 'REAL'
        elif dtype_.startswith('datetime'):
            return 'TIMESTAMP'
        elif dtype_ == 'bool':
            return 'BOOLEAN'
        else:
            return 'VARCHAR(256)'

    def _get_column_data_types(self, df: DataFrame, index: bool = False) -> list:
        """Retrieves redshift compatible data types from a DataFrame"""
        column_data_types = [self._to_redshift_types(dtype_.name) for dtype_ in df.dtypes.values]
        if index:
            column_data_types.insert(
                0, self._to_redshift_types(df.index.dtype.name))
        return column_data_types

    def _s3_to_redshift(self,
                        redshift_table_name: str,
                        column_list: list,
                        s3_bucket_name: str,
                        s3_key: str,
                        delimiter: str = ',',
                        quotechar: str = '"',
                        dateformat: str = 'auto',
                        timeformat: str = 'auto',
                        region: str = '',
                        parameters: str = '',
                        aws_role: str = None,
                        aws_token: str = ''):
        """Executes a COPY command from Redshift to load data from S3"""

        # get authentication information
        s3_file_path = f's3://{s3_bucket_name}/{s3_key}'
        access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

        # create query parts
        if aws_role:
            authorization = f"""
            IAM_ROLE '{aws_role}'
            """
        elif access_key_id and secret_access_key:
            authorization = f"""
            access_key_id '{access_key_id}'
            secret_access_key '{secret_access_key}'
            """
        else:
            logger.warning("no AWS authentification set")
            authorization = ""

        s3_to_sql = f"""
        COPY {redshift_table_name}({' ,'.join(column_list)})
        FROM '{s3_file_path}'
        DELIMETER '{delimiter}'
        ignoreheader 1
        GZIP csv quote as '{quotechar}'
        dateformat '{dateformat}'
        timeformat '{timeformat}'
        {authorization}
        {parameters}
        """

        if region:
            s3_to_sql = s3_to_sql + f"region '{region}'"

        if aws_token:
            s3_to_sql = s3_to_sql + f"\n\tsession_token '{aws_token}'"
        s3_to_sql = s3_to_sql + ';'

        # log the whole resulting request
        logger.debug(s3_to_sql)

        # send the file
        logger.info('FILLING THE TABLE IN REDSHIFT')
        try:
            self.cursor.execute(s3_to_sql)
            self.connector.commit()
        except Exception as e:
            logger.error(e)
            traceback.print_exc(file=sys.stdout)
            self.connector.rollback()
            raise

    def _create_redshift_table(self,
                               df: DataFrame,
                               redshift_table_name: str,
                               column_data_types: list = None,
                               column_constraints: list = None,
                               index: bool = False,
                               diststyle: str = 'even',
                               distkey: str = '',
                               sort_interleaved: bool = False,
                               sortkey: str = '',
                               include_date_insert: bool = True,
                               debug=False):
        """Create a Redshift table based on a schema build from a DataFrame object"""

        columns = list(df.columns)

        if index:
            if df.index.name:
                columns.insert(0, df.index.name)
            else:
                columns.insert(0, "index")

        if column_data_types is None:
            column_data_types = self._get_column_data_types(df, index)

        columns_and_data_type = ', '.join(
            [f'{x} {y}' for x, y in zip(columns, column_data_types)])
        if include_date_insert:
            columns_and_data_type += ', date_insert DATETIME DEFAULT GETDATE()'

        create_table_query = f'CREATE TABLE IF NOT EXISTS {redshift_table_name} \
                                ({columns_and_data_type}'

        if column_constraints is not None:
            create_table_query += " ," + ", ".join(column_constraints) + ")"
        else:
            create_table_query += ")"

        if not distkey:
            # Without a distkey, we can set a diststyle
            if diststyle not in ['even', 'all', 'auto']:
                raise ValueError("diststyle must be either 'even', 'all' or 'auto'")
            else:
                create_table_query += f' DISTSTYLE {diststyle}'
        else:
            create_table_query += ' DISTSTYLE key'
            create_table_query += f' DISTKEY({distkey})'
        if len(sortkey) > 0:
            if sort_interleaved:
                create_table_query += ' INTERLEAVED'
            create_table_query += f' SORTKEY({sortkey})'
        if debug:
            logger.debug('CREATING A TABLE IN REDSHIFT')
            logger.debug(create_table_query)

        self.cursor.execute(create_table_query)
        self.connector.commit()

    def _pandas_to_redshift(self,
                            df: DataFrame,
                            redshift_table_name: str,
                            s3_bucket_name: str,
                            s3_key_prefix: str,
                            column_data_types: list = None,
                            column_constraints: list = None,
                            index: bool = False,
                            diststyle: str = 'even',
                            distkey: str = '',
                            sort_interleaved: bool = False,
                            sortkey: str = '',
                            delimiter: str = ',',
                            quotechar: str = '"',
                            dateformat: str = 'auto',
                            timeformat: str = 'auto',
                            region: str = '',
                            parameters: str = '',
                            aws_role: str = '',
                            aws_token: str = '',
                            drop_table: bool = False,
                            debug: bool = False
                            ):
        """Private method to create and load data from a DataFrame to a Redshift table"""

        df = self._validate_column_names(df)

        d = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        s3_key = f"{s3_key_prefix}/{redshift_table_name.replace('.', '/')}/{d}.csv.gz"
        put_df(self.s3_client, df, s3_bucket_name, s3_key, format='csv', compression='gzip')

        if drop_table:
            logger.info(">>Droping table")
            self.cursor.execute(f'DROP TABLE IF EXISTS {redshift_table_name} CASCADE;')

        self._create_redshift_table(df,
                                    redshift_table_name,
                                    column_data_types=column_data_types,
                                    column_constraints=column_constraints,
                                    index=index,
                                    diststyle=diststyle,
                                    distkey=distkey,
                                    sort_interleaved=sort_interleaved,
                                    sortkey=sortkey,
                                    debug=debug
                                    )

        self._s3_to_redshift(redshift_table_name,
                             list(df.columns),
                             s3_bucket_name,
                             s3_key,
                             delimiter,
                             quotechar,
                             dateformat,
                             timeformat,
                             region,
                             parameters,
                             aws_role,
                             aws_token)

    def upload_to_redshift(self,
                           df: DataFrame,
                           redshift_table_name: str,
                           s3_bucket_name: str,
                           s3_key_prefix: str,
                           aws_role: str,
                           debug: bool = False,
                           **kwargs
                           ):
        """Public method to create and load data from a DataFrame to a Redshift table"""

        logger.info("Uploading to Redshift")
        try:
            self._pandas_to_redshift(
                df=df,
                redshift_table_name=f'{self.schema}.{redshift_table_name}',
                s3_bucket_name=s3_bucket_name,
                s3_key_prefix=s3_key_prefix,
                aws_role=aws_role,
                debug=debug,
                **kwargs
            )
        except TypeError:
            raise TypeError('upload_to_redshift() got an unexpected keyword argument when calling pandas_to_redshift()')

        logger.info("Data loaded to Redshift")

    def _create_temp_redshift_table_from_target(self, target_redshift_table_name):
        """Create a temporary table based on a target table exisitng in redshift"""

        self.cursor.execute(f'DROP TABLE IF EXISTS stage_{target_redshift_table_name}'
        create_table_query = f'CREATE TEMP TABLE stage_{target_redshift_table_name} (LIKE {target_redshift_table_name})'

        logger.info('CREATING A TABLE IN REDSHIFT')

        self.cursor.execute(create_table_query)
        self.cursor.execute(f'ALTER TABLE stage_{target_redshift_table_name} DROP COLUMN date_insert;')
        self.connector.commit()

    def _delete_target_redshift_table_line(self, origin_table_name: str, target_table_name: str, key: list):
        """Delete rows in a Redshift table based on another table content"""

        comparison = "WHERE " + " AND".join([f' {target_table_name}.{v} = {origin_table_name}.{v}' for v in key]) + ";"
        query = f"""
            BEGIN TRANSACTION;
            DELETE FROM {target_table_name}
            USING {origin_table_name}
            {comparison};
        """

        logger.debug(f'DELETE LINES IN REDSHIFT TABLE {target_table_name}')

        self.cursor.execute(query)
        self.connector.commit()

    def _insert_target_redshift_table_line(
            self,
            origin_table_name,
            target_table_name,
            drop_origin_table: bool = True
            ):
        """Insert rows in a Redshift table based on another table content"""

        query = f"""
            INSERT INTO {target_table_name}
            SELECT * FROM {origin_table_name};
        """
        if drop_origin_table:
            query += f"\n\tDROP TABLE {origin_table_name};"

        query += "\n\tEND TRANSACTION;"

        logger.debug(f'INSERT LINES IN REDSHIFT TABLE {target_table_name}')

        self.cursor.execute(query)
        self.connector.commit()

    def upsert_rows(
                    self,
                    update_df: DataFrame,
                    target_table_name: str,
                    s3_bucket_name: str,
                    s3_key_prefix: str,
                    comparison_key: list,
                    aws_role: str,
                    **kwargs):
        """Performs an upsert lines into a target Redshift table based on a DataFrame content"""

        d = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        update_df = self._validate_column_names(update_df)
        s3_key = f"{s3_key_prefix}/{self.schema}/{target_table_name}/{d}.csv.gz"
        put_df(self.s3_client, update_df, s3_bucket_name, s3_key, format='csv', compression='gzip')
        self._create_temp_redshift_table_from_target(target_table_name)
        self._s3_to_redshift(f'stage_{target_table_name}',
                             list(update_df.columns),
                             s3_bucket_name,
                             s3_key,
                             aws_role=aws_role
                             )
        # the following method also begins a transaction
        self._delete_target_redshift_table_line(f'stage_{target_table_name}',
                                                target_table_name,
                                                comparison_key
                                                )
        # the following method also ends the started transaction, and drop the staging table
        self._insert_target_redshift_table_line(f'stage_{target_table_name}',
                                                target_table_name
                                                )
