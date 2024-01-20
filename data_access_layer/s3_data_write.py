import pandas as pd
import boto3
from boto3.session import Session
from configparser import ConfigParser
import constants
import os
import json
from io import StringIO  # python3; python2: BytesIO
import boto3
import io


class S3DataWriter:
    def __init__(self) -> None:
        config = ConfigParser()
        config.read(constants.AWS_CREDNTIALS_FILE)
        self.aws_access_key = config.get("default", "aws_access_key_id")
        self.aws_secret_key = config.get("default", "aws_secret_access_key")

    def connect_s3(self):
        """
        @param ACCESS_KEY:
        @type ACCESS_KEY:
        @param SECRET_KEY:
        @type SECRET_KEY:
        @return:
        @rtype:
        """
        try:
            session = Session(
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
            )
        except:
            print("Not connected")
            return
        s3 = session.resource("s3")
        print("AWS connection successful")
        return s3

    def write_s3_csv(self, df, bucket, filepath, sep):
        csv_buffer = StringIO()
        try:
            df.to_csv(csv_buffer, sep=",")
            s3_resource = boto3.resource("s3")
            s3_resource.Object(bucket, filepath).put(Body=csv_buffer.getvalue())
            print("File Uploaded!!")
            return True
        except:
            print("File Not Uploaded!!")
            return False

    def write_s3_parquet(
        self, df, bucketName, keyName, s3=None, s3_client=None, verbose=False, **args
    ):
        """
        @param df:
        @type df:
        @param bucketName:
        @type bucketName:
        @param keyName:
        @type keyName:
        @param s3:
        @type s3:
        @param s3_client:
        @type s3_client:
        @param verbose:
        @type verbose:
        @param args:
        @type args:
        """
        if s3_client is None:
            s3_client = boto3.client("s3")
        if s3 is None:
            s3 = boto3.resource("s3")
        try:
            csv_buf = io.BytesIO()
            df.to_parquet(csv_buf, index=False)
            s3_client.put_object(
                Bucket=bucketName, Key=keyName, Body=csv_buf.getvalue()
            )
            print("parquet file created. Please verify the S3 location once.")
        except Exception as e:
            print("Failed with error: " + str(e))


if __name__ == "__main__":
    sc = S3DataWriter()
    df = pd.read_csv("/Users/nyzy/Downloads/FRB_G17.csv")
    # print(sc.write_s3_csv(df,"allianceware","rest.csv"))
    # sc.write_s3_parquet(df,"allianceware","rest.parquet")
