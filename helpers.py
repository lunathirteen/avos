import pandas as pd
from sqlalchemy import create_engine, text


class DatabaseManager:
    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)

    def query_to_df(self, sql_query):
        return pd.read_sql(sql_query, con=self.engine)

    def execute_query(self, sql_query):
        with self.engine.connect() as connection:
            return connection.execute(text(sql_query))

    def write_to_db(self, df, table_name, if_exists="replace"):
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
