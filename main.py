import os
import sys
import constants
from data_access_layer import postgres_data_read
from data_access_layer.gouri_learning import ReadJsonLocalS3


class SportsAnalysis:
    def __init__(self) -> None:
        rj = ReadJsonLocalS3()
        rj.read_json_local("gouri is amazing")

    def start(self):
        try:
            print(2 / 0)

        except Exception as e:
            print("Exception raised", e)

        finally:
            print("Final executed")


if __name__ == "__main__":
    sports_driver = SportsAnalysis()
    sports_driver.start()
