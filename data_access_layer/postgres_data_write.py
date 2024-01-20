import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os.path, sys

# sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
import constants


class PostgresSqlWriter:
    def __init__(self):
        self.engine = create_engine(
            "postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(
                constants.POSTGRES_USER,
                constants.POSTGRES_PASSWORD,
                constants.POSTGRES_HOST,
                constants.POSTGRES_PORT,
                constants.POSTGRES_DATABASE,
            )
        )
        self.conn = self.engine.connect()

    def write_table(
        self,
        df,
        table,
        schema=None,
        if_exists="append",
        index=False,
        dtype=None,
        chunksize=10000,
    ):
        df.to_sql(
            table,
            con=self.conn,
            if_exists=if_exists,
            schema=schema,
            index=False,
            dtype=dtype,
        )
        return df


if __name__ == "__main__":
    sc = PostgresSqlWriter()
