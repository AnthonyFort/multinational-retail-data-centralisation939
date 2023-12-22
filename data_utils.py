import os
import yaml
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, inspect

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

  '''
  This class provides the methods to connect with the initial, dispersed databases, 
  and the methods to upload to the new, centralised database.
  '''

  def __init__(self):
    '''
    The attributes ensure that, once ascertained or created, the database credentials
    and databased engines can be accessed readily.
    '''
    self.creds_dict = None
    self.engine = None
    self.upload_engine = None

  def read_db_creds(self, creds):
    '''
    This method reads the credentials which are held in a separate yaml which is listed
    in the gitignore file (and, hence, not directly accessible).
    '''
    try:
      with open(creds, 'r') as f:
        self.creds_dict = yaml.safe_load(f)
    except Exception as e:
      print(e)    
      
  def init_db_engine(self):
    '''
    This method creates the engine that will allow for the data to be extracted from the initial,
    dispersed databases.
    '''
    try:
      self.engine = create_engine(f"postgresql://{self.creds_dict['RDS_USER']}:{self.creds_dict['RDS_PASSWORD']}@{self.creds_dict['RDS_HOST']}:{self.creds_dict['RDS_PORT']}/{self.creds_dict['RDS_DATABASE']}")
    except Exception as e:
      print(e)

  def init_upload_engine(self):
    '''
    This method creates the engine that will be used when uploading to the new, centralised databased.
    The credentials for this database are held in a .env file (and, hence, are not directly accessible).
    '''
    try:
      self.upload_engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    except Exception as e:
      print(e)      
  
  def list_db_tables(self):
    '''
    This method allows the programmer to read the names of the different tables he/she is
    pulling from the initial sources.
    Knowing the names of the tables is essential for data extraction.
    '''
    try:
      with self.engine.connect() as conn:
        inspection = inspect(conn)
        return inspection.get_table_names()
    except Exception as e:
      print(e)  

  def upload_to_db(self, df, table_name):
    '''
    This method is to be used after the data has been cleaned, 
    and uploads the cleaned data to the new database.
    '''
    try:
      with self.upload_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
        df.to_sql(table_name, conn, if_exists='replace')   
    except Exception as e:
      print(e)
        