import pandas as pd
import numpy as np
import json
import constants


class ReadJsonLocalS3:
    def __init__(self) -> None:
        print("gouri is in constructor")

    def read_json_local(self, variable):
        print(constants.JSON_FILE_DIR)


if __name__ == "__main__":
    rj = ReadJsonLocalS3()
    rj.read_json_local("gouri is cool")

    print("gouri is in main")
