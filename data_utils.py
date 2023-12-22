import yaml
from sqlalchemy import create_engine, inspect

from dotenv import find_dotenv, load_dotenv
import os
load_dotenv(find_dotenv())

DATABASE_TYPE = os.environ.get('DATABASE_TYPE')
DBAPI = os.environ.get('DBAPI')
HOST = os.environ.get('HOST')
# USER = os.environ.get('USER')
USER = 'postgres'
PASSWORD = os.environ.get('PASSWORD')
DATABASE = os.environ.get('DATABASE')
PORT = 5432

engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
engine.execution_options(isolation_level='AUTOCOMMIT').connect()

class DatabaseConnector():

  def __init__(self):
    self.creds_dict = None
    self.engine = None
    self.upload_engine = None

  def read_db_creds(self, creds):
    try:
      with open(creds, 'r') as f:
        self.creds_dict = yaml.safe_load(f)
    except Exception as e:
      print(e)    
      
  def init_db_engine(self):
    try:
      self.engine = create_engine(f"postgresql://{self.creds_dict['RDS_USER']}:{self.creds_dict['RDS_PASSWORD']}@{self.creds_dict['RDS_HOST']}:{self.creds_dict['RDS_PORT']}/{self.creds_dict['RDS_DATABASE']}")
    except Exception as e:
      print(e)

  def init_upload_engine(self):
    try:
      self.upload_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    except Exception as e:
      print(e)      
  
  def list_db_tables(self):
    try:
      with self.engine.connect() as conn:
        inspection = inspect(conn)
        return inspection.get_table_names()
    except Exception as e:
      print(e)  

  def upload_to_db(self, df, table_name):
    try:
      with self.upload_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        df.to_sql(table_name, conn, if_exists='replace')   
    except Exception as e:
      print(e)
        