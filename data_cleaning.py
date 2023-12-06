import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from dotenv import find_dotenv, load_dotenv
import os
from dateutil.parser import parse
import data_extraction

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

link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

df_from_extraction = data_extraction.new_df.retrieve_pdf_data(link)

class DataCleaning:

  def clean_card_data(self, df):
    df.replace(to_replace = 'NULL', value=None, inplace=True)
    df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
    df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%d', errors='coerce')
    df.dropna(inplace=True, ignore_index=True)
    df['expiry_date'] = df['expiry_date'].astype(str).str[5:]
    df['card_number'] = df['card_number'].astype(str).str.strip().str.upper()
    df['card_provider'] = df['card_provider'].str.strip().str.upper()
    self.upload_to_db(df)

  def upload_to_db(self, df):
    df.to_sql('dim_card_details', engine, if_exists='replace')  
    

  def clean_store_data(self, filename):
    df = pd.read_json(filename)
    df['address'] = df['address'].replace('\n', ' ', regex=True).str.strip().str.upper()
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
    df = df.drop('lat', axis=1)
    df['locality'] = df['locality'].str.strip().str.upper()
    df['store_code'] = df['store_code'].str.strip()
    df = df[df['store_code'].str[2].eq('-')]
    df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
    df = df.dropna(subset=['staff_numbers'])
    df['staff_numbers'] = df['staff_numbers'].astype('int32')
    df['opening_date'] = df['opening_date'].apply(parse)
    df['opening_date'] = pd.to_datetime(df['opening_date'], infer_datetime_format=True, errors='coerce')
    df['store_type'] = df['store_type'].str.strip().str.upper()
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['continent'] = df['continent'].replace('ee', '', regex=True).str.strip().str.upper()
    df = df.drop('message', axis=1)
    df.to_sql('dim_store_details', engine, if_exists='replace')

df_to_clean = DataCleaning()

# df_to_clean.clean_card_data(df_from_extraction)

df_to_clean.clean_store_data('stores_data.json')