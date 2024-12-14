from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import pyodbc

class ConnectionHandler:
    def __init__(self,host=None,port=None,db=None,user=None,password=None) -> None:
        if(host is None):
            self.host="localhost"
        else:
            self.host = host
        
        if(port is None):
            self.port="3306"
        else:
            self.port=port
        
        if(db is None):
            self.db="sra_vm"
        else:
            self.db=db
        
        if(user is None):
            self.user="root"
        else:
            self.user = user
        
        if(password is None):
            self.password="123456"
        else:
            self.password = password
        
        # LAPTOP-E6M421LQ\DHINA

        #driver = '{ODBC Driver 17 for SQL Server}'
        #driver = 'ODBC Driver 17 for SQL Server'

        #connection_string=f'Driver={driver};Server={self.host},PORT=1433;DATABASE={self.db};UID={self.user};PWD={self.password}'
        #connection_string=f'Driver={driver};Server={self.host};DATABASE={self.db};UID={self.user};PWD={self.password};&autocommit=true'
        self.connection_url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
            self.user, self.password, self.host, self.port, self.db)
        
        self.engine=create_engine(self.connection_url)
        #print(connection_string)
        print(self.connection_url)
       
    def get_connection_url(self):
        return self.engine

    def fetch_data(self,query):
        with self.engine.connect() as connection:
            return pd.read_sql(query,con=connection)
    
    def insert_data(self,df,tablename):
        with self.engine.connect() as connection:
            
            df.reset_index(inplace=True,drop=True)
            df.to_sql(tablename,if_exists='append',index=False,con=connection)
        return True 
    
    def execute_query(self,query):
        result=""
        with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            result = connection.execute(text(query))
            print(f"Rows affected {result.rowcount}")
        return result
    
    def __del__(self):
        try:
            self.db_connection.close()
        except:
            None

obj_connection_handler = ConnectionHandler()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=obj_connection_handler.engine)

Base = declarative_base()

# #print(pyodbc.drivers())

# #dal = ConnectionHandler('LAPTOP-E6M421LQ\\DHINA','test')
# #dal = ConnectionHandler('LAPTOP-E6M421LQ\\DHINA;','test','dhina','dhina123')
# dal = ConnectionHandler()
# #dal.execute_query("update profile set name='sra_vm' where id=3")
# df = dal.fetch_data("select * from profile")
# print(df)
# dal.execute_query("delete from profile where id=5")
# df = dal.fetch_data("select * from profile")
# print(df)