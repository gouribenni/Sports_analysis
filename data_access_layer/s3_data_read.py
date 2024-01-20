import pandas as pd
import boto3
from boto3.session import Session
from configparser import ConfigParser
import constants
import os
import json


class S3DataReader:
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

    def read_s3_folders(self, bucket_name, prefix):
        """
        @param bucket_name:
        @type bucket_name:
        @param prefix:
        @type prefix:
        """
        client = boto3.client("s3")
        result = client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

        folder_list = []
        for o in result.get("CommonPrefixes"):
            print("sub folder : ", o.get("Prefix"))
            if "_delta_log" not in o.get("Prefix"):
                folder_list.append(o.get("Prefix"))
        return folder_list

    def read_s3_files(self, bucket_name, prefix):
        """
        @param bucket_name:
        @type bucket_name:
        @param prefix:
        @type prefix:
        """
        file_list = []
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(name=bucket_name)
        FilesNotFound = True
        for obj in bucket.objects.filter(Prefix=prefix):
            file_list.append(os.path.join(obj.key))
            # print('{0}:{1}'.format(bucket.name, obj.key))
            FilesNotFound = False
        if FilesNotFound:
            print("ALERT", "No file in {0}/{1}".format(bucket, prefix))
        return file_list

    def read_s3_csv(
        self, bucket, filepath, sep=",", s3=None, s3_client=None, verbose=False, **args
    ):
        """
        @param filepath:
        @type filepath:
        @param bucket:
        @type bucket:
        @param sep:
        @type sep:
        @param s3:
        @type s3:
        @param s3_client:
        @type s3_client:
        @param verbose:
        @type verbose:
        @param args:
        @type args:
        @return:
        @rtype:
        """
        if s3_client is None:
            s3_client = boto3.client("s3")
        if s3 is None:
            s3 = boto3.resource("s3")
        try:
            csv_obj = s3_client.get_object(Bucket=bucket, Key=filepath)
            body = csv_obj["Body"]
            df_main = pd.read_csv(body, sep=sep)
        except Exception as e:
            print("Failed with error: " + str(e))
        return df_main

    def read_s3_json(
        self, bucket, filepath, s3=None, s3_client=None, verbose=False, **args
    ):
        """
        @param filepath:
        @type filepath:
        @param bucket:
        @type bucket:
        @type verbose:
        @param args:
        @type args:
        @return:
        @rtype:
        """
        s3 = boto3.resource("s3")
        obj = s3.Object(bucket, filepath)
        data = json.load(obj.get()["Body"])
        return data

    def read_s3_parquet(self, file_path_on_s3):
        """
        @param path:
        @type path:
        @return:
        @rtype:
        """
        try:
            df = pd.read_parquet(file_path_on_s3, engine="pyarrow")
            print("Data Read Successfully")
            return df
        except Exception as e:
            print("Failed with error: " + str(e))


if __name__ == "__main__":
    sc = S3DataReader()
    # sc.connects3()
    # sc.read_s3_folders("sports-analysis-project","raw_files/")
    # print(sc.read_s3_files("sports-analysis-project","raw_files/"))
    # print(sc.read_s3_csv("allianceware","FRB_G17.csv",","))
    # print(sc.read_s3_json("sports-analysis-project","raw_files/india_male_json/1022597.json"))
    # print(sc.read_s3_parquet("s3://allianceware/rest.parquet"))
