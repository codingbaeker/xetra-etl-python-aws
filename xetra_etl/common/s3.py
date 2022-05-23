"""Connector and methods accessing S3"""
from io import BytesIO, StringIO
import os
import logging

import boto3
import pandas as pd

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
        self.endpoint_url = endpoint_url
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

    def read_csv_as_df(self, bucket: str, key: str, decoding: str = 'utf-8', sep: str = ','):
        """
        read csv file from bucket and put its data in a dataframe

        :param bucket: S3 bucket name
        :param key:  key of csv file in S3 bucket
        :param decoding: encoding method
        :param sep: delimiter of the file

        returns:
            dataframe: dataframe with the data of the csv file
        """
        csv_obj = bucket.Object(key=key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        dataframe = pd.read_csv(data, delimiter=sep)
        return dataframe

    def write_df_to_s3(self, bucket: str, dataframe: str, key: str):
        """
        writes dataframe to S3 Bucket

        :param bucket: S3 bucket name
        :param dataframe: Pandas dataframe
        :param key: key to save file in S3 Bucket
        """
        out_buffer = BytesIO()
        dataframe.to_parquet(out_buffer, index=False)
        bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True
        
