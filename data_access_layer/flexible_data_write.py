import pandas as pd
import boto3
from boto3.session import Session
from configparser import ConfigParser
import constants
import os
import json
import s3fs


class FlexDataWrite:
    def __init__(self) -> None:
        None

    def write_csv(self, df, path, sep=","):
        """
        write csv
        """
        df = pd.write_csv(path, sep=sep)
        return True

    def write_json(self, df, path, orient="split", compression="infer", index="true"):
        """
        write json
        """

        df.to_json(path, orient=orient, compression=compression, index=index)
        return True


# returns JSON object as
# a dictionary
