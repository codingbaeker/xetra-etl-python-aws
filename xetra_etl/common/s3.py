"""Connector and methods accessing S3"""
from io import BytesIO, StringIO
import os
import logging

import boto3
import pandas as pd

from xetra_etl.common.constants import S3FileTypes
from xetra_etl.common.custom_exeptions import WrongFormatException

class S3BucketConnector():
    """
    Class for interacting with S3 Buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self._endpoint_url = endpoint_url
        self.session = boto3.Session(
                                aws_access_key_id=os.environ[access_key],
                                aws_secret_access_key=os.environ[secret_key]
                                )
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        listing all files with a prefix on the S3 bucket

        :param prefix: prefix on the S3 bucket that should be filtered with

        returns:
            files: list of all the file names containing the prefix as a key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_as_df(self, key: str, encoding: str = 'utf-8', sep: str = ','):
        """
        read csv file from bucket and put its data in a dataframe

        :param key:  key of csv file in S3 bucket
        :param decoding: encoding method
        :param sep: delimiter of the csv file

        returns:
            dataframe: dataframe with the data of the csv file
        """
        self._logger.info('Reading file %s/%s/%s', self._endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, delimiter=sep)
        return data_frame

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str):
        """
        writes dataframe to S3 Bucket

        :param bucket: S3 bucket name
        :param dataframe: Pandas dataframe
        :param key: key to save file in S3 Bucket
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty, nothing will be written')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        
        self._logger.info('File Format %s is not supported', file_format)
        
        raise WrongFormatException
        
    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()
        
        :param out_buffer: StringIO | BytesIO that should be written
        :param key: target key of the saved file
        """
        self._logger.info('Writing file to %s/%s/%s', self._endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True