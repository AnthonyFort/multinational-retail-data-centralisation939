import yaml
from sqlalchemy import create_engine, inspect
import os
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine

load_dotenv(find_dotenv())

DATABASE_TYPE = os.environ.get('DATABASE_TYPE')
DBAPI = os.environ.get('DBAPI')
HOST = os.environ.get('HOST')
# USER = os.environ.get('USER')
USER = 'postgres'
PASSWORD = os.environ.get('PASSWORD')
DATABASE = os.environ.get('DATABASE')
PORT = 5432

upload_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
upload_engine.execution_options(isolation_level='AUTOCOMMIT').connect()


class DatabaseConnector:

  def __init__(self):
      self.engine = None
  
  def read_db_creds(self, creds):
    with open(creds, 'r') as f:
      creds_dict = yaml.safe_load(f)
      return self.init_db_engine(creds_dict)

  def init_db_engine(self, creds):
    self.engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
    self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
    self.list_db_tables(self.engine)
    return self.engine

  def list_db_tables(self, engine):
    inspection = inspect(engine)
    print(inspection.get_table_names())
    return engine
    
  def upload_to_db(self, df, table_name):
    df.to_sql(table_name, upload_engine, if_exists='replace') 

new_db_connector = DatabaseConnector()

# new_db_connector.read_db_creds('db_creds.yaml')