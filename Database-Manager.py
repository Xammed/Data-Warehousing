import pymysql
import sqlalchemy
from google.cloud.sql.connector import connector
import pandas as pd

class DatabaseManager:
    def __init__(self, name, user, pw, db, cloud=True):
        if cloud:
            self.engine = self.init_connection_engine(name, user, pw, db)
            self.conn = self.engine.raw_connection()
            self.cursor = self.conn.cursor()
        else:
            self.cursor = self.initialize_host_connection(name, user, pw, db)
            
    
    def init_connection_engine(self, name, u, p, db):
        def getconn():
            conn: pymysql.connections.Connection = connector.connect(
                name,
                "pymysql",
                user=u,
                password = p,
                db=db
            )
            return conn

        engine = sqlalchemy.create_engine(
            "mysql+pymysql://",
            creator=getconn,
        )
        return engine

    
    def initialize_host_connection(self, name, u, p, db):
        self.conn = pymysql.connect(
            host = name,
            port=3306,
            user = u,
            password = p,
            db = db
        )
        self.cursor = self.conn.cursor()
        return self.cursor

    def action_query(self, q):
        self.cursor.execute(q)
        self.conn.commit()
        return 0

    def query_pd(self, q):
        return pd.read_sql_query(q, self.conn)


    def insert_row(self, table, row, printing=False):
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join(row.keys())
        query = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table, columns, placeholders)
        self.cursor.execute(query, list(row.values()))
        if printing:
            print("Success on: " + str(row))
        self.conn.commit()


    def show_keys(self, tables):
        for table in tables:
            self.cursor.execute('SHOW COLUMNS FROM {table}'.format(table=table))
            print(self.cursor.fetchall())
        return 0

    def close_cursor(self):
        self.cursor.close()
        return 0




#EXAMPLE USAGE:
'''
c = DatabaseManager("clover-pos-intern-2021-sandbox:us-central1:mock-crash-db","root",None,"meta",True)
names = ['android_version', 'device_app', 'unified_report', 'unified_report_group', 'developer_app']
def fucked_up():
    for name in names:
        c.action_query("DELETE FROM {X}".format(X=name))
        d = c.query_pd("SELECT * FROM {X}".format(X=name))
        print(d)
#c.insert_row('foo', {'name':'Grape', 'age':73})
#c.action_query('DELETE FROM android_version;')
d = c.query_pd('SELECT * FROM device_app;')
print(d)
#
fucked_up()
'''