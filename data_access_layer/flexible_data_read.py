import pandas as pd
import boto3
from boto3.session import Session
from configparser import ConfigParser
import constants
import os
import json
import s3fs


class FlexDataRead:
    def __init__(self) -> None:
        self.fs = s3fs.S3FileSystem()

    def read_csv(self, path):
        """
        read csv and return pandas dataframe
        """
        df = pd.read_csv(path)
        return df

    def read_json(self, path, s3_path=False):
        """
        read JSON and return JSON object
        """
        if s3_path:

            with self.fs.open(s3_path, "rb") as f:
                data = json.load(f)
            return data
        f = open(path)
        data = json.load(f)
        return data

    def read_s3_file_structure(self, path):
        return self.fs.ls(path)


# returns JSON object as
# a dictionary
