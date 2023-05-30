import os
import pandas as pd
import logging
from sqlalchemy import create_engine
import pymysql

class MysqlConnector:
    def __init__(self) -> None:
        pymysql.install_as_MySQLdb()

        host = os.getenv('host')
        user = os.getenv('user')
        port = int(os.getenv('port'))
        password = os.getenv('password')
        database = os.getenv('database')

        self.connection = pymysql.connect(host=host, user=user, password=password, db=database, port=port)
        self.cursor = self.connection.cursor()

        engine = create_engine(f"mysql+mysqldb://{user}:{password}@{host}:{port}/{database}", encoding='utf-8')
        self.conn = engine.connect()

        pass

    def getTableColumns(self, table_name):
        query = f"""
            SHOW COLUMNS FROM {table_name};
        """
        self.cursor.execute(query, [])
        rows = self.cursor.fetchall()
        if rows:
            result = []
            for row in rows:
                if row[0] == 'id':
                    continue
                if row[0] == 'created_at':
                    continue
                if row[0] == 'updated_at':
                    continue
                result.append(row[0])
            return result
        return None

    def upsertTableData(self, **args):
        table_name = args['table_name']
        table_columns = self.getTableColumns(table_name)
        data_results = args['data_results']

        # dataframe
        data_df = pd.DataFrame(data_results, columns=table_columns)

        # make query
        values = ""
        query = f"""
            INSERT INTO {table_name} ( """
        for columns in table_columns:
            query+= f""" `{columns}`, """
            values+= "%s,"
        query = query[:-2]
        query+= " )"

        values = values[:-1]
        query+= f""" VALUES ({values}) """
        query+= " ON DUPLICATE KEY UPDATE "

        for columns in table_columns:
            query+= f""" `{columns}`=VALUES(`{columns}`), """
            values+= "%s,"
        query = query[:-2]

        # upsert
        self.upsertData(query, data_df)
        self.autoincrement_init(table_name)

        return None
    def autoincrement_init(self, table_name):
        query = f""" ALTER TABLE {table_name} AUTO_INCREMENT = 1 """
        self.cursor.execute(query, [])

        return None

    def upsertData(self, query, response_df):
        response_df=response_df.fillna('')
        arg=response_df.values.tolist()
        self.cursor.executemany(query, arg)
        self.connection.commit()

        print({'result': f'{len(response_df)} records are upserted'})
        return None

    def connectClose(self):
        self.connection.close()
        self.conn.close()
        pass

    def read_query_row(self, **args):
        self.cursor.execute(args['query'], args['param'])
        row = self.cursor.fetchone()
        if row:
            columns = [column[0] for column in self.cursor.description]
            return dict(zip(columns, row))
        return None

    def commit_query(self, query, param):
        self.cursor.execute(query, param)
        self.connection.commit()
