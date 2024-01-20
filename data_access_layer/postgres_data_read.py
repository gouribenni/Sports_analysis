import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os.path, sys

# sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
import constants

# from load import test
class PostgresSqlReader:
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

    def read_table(self, table, schema=None, chunksize=10000):
        chunks = pd.read_sql_table(table, self.conn, schema=schema, chunksize=chunksize)
        df = pd.concat(list(chunks))
        return df

    def read_sql_query(self, query, schema=None, chunksize=10000):
        chunks = pd.read_sql_query(query)
        df = pd.concat(list(chunks))
        return df


# df = pd.read_sql("select * from public.supply_chain", con = engine)

# print(df.shape)


if __name__ == "__main__":
    sc = PostgresSqlReader()
