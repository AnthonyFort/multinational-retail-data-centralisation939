import pandas as pd
import numpy as np
from dateutil.parser import parse
from dateutil.parser._parser import ParserError
from dataprep.clean import clean_email
from data_extraction import users_df, orders_df
from data_utils import new_db_connector
from dotenv import find_dotenv, load_dotenv

class DataCleaning():
  
  def parse_date(self, date_column):
    if pd.isna(date_column):
      return None
    try:
      return parse(date_column)
    except (ParserError, TypeError):
      return None

  def clean_user_data(self, df):
    df['first_name'] = df['first_name'].str.title().str.strip().str.replace('[^a-zA-Z]', '', regex=True)
    df['last_name'] = df['last_name'].str.title().str.strip().str.replace('[^a-zA-Z]', '', regex=True)
    df['date_of_birth'] = df['date_of_birth'].apply(self.parse_date)
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], infer_datetime_format=True, errors='coerce')
    df['company'] = df['company'].str.strip()
    clean_email(df, 'email_address', remove_whitespace=True)
    df['address'] = df['address'].replace('\n', ' ', regex=True).str.strip()
    valid_countries = ['Germany', 'United Kingdom', 'United States']
    df.loc[~df['country'].isin(valid_countries), 'country'] = np.nan
    valid_country_codes = ['DE', 'GB', 'US']
    df.loc[~df['country_code'].isin(valid_country_codes), 'country_code'] = np.nan
    df.loc[df['country'] == 'United Kingdom', 'country_code'] = 'GB'
    # countries_without_code = df[df['country_code'].isna() & df['country'].notna()]
    # codes_without_country = df[df['country'].isna() & df['country_code'].notna()]
    # print(countries_without_code[['country', 'country_code']])
    # print(codes_without_country[['country', 'country_code']])
    df['phone_number'] = df['phone_number'].str.replace('[^\d+]', '', regex=True)
    df['join_date'] = df['join_date'].apply(self.parse_date)
    df['join_date'] = pd.to_datetime(df['join_date'], infer_datetime_format=True, errors='coerce')
    df['user_uuid'] = df['user_uuid'].mask(~df['user_uuid'].str[8].eq('-'), other=np.nan)
    df['user_uuid'] = df['user_uuid'].mask(~df['user_uuid'].str[13].eq('-'), other=np.nan)
    df['user_uuid'] = df['user_uuid'].mask(~df['user_uuid'].str[18].eq('-'), other=np.nan)
    df['user_uuid'] = df['user_uuid'].mask(~df['user_uuid'].str[23].eq('-'), other=np.nan)
    df.drop_duplicates(inplace=True)
    df = df.dropna()
    return new_db_connector.upload_to_db(df, 'dim_users')
      
  def clean_orders_data(self, df):
    df.drop('first_name', axis=1, inplace=True)
    df.drop('last_name', axis=1, inplace=True)
    df.drop('1', axis=1, inplace=True)
    df.drop('level_0', axis=1, inplace=True)
    df['date_uuid'] = df['date_uuid'].mask(~df['date_uuid'].str[8].eq('-'), other=np.nan)
    df['date_uuid'] = df['date_uuid'].mask(~df['date_uuid'].str[13].eq('-'), other=np.nan)
    df['date_uuid'] = df['date_uuid'].mask(~df['date_uuid'].str[18].eq('-'), other=np.nan)
    df['date_uuid'] = df['date_uuid'].mask(~df['date_uuid'].str[23].eq('-'), other=np.nan)
    df['user_uuid'] = df['user_uuid'].mask(~df['user_uuid'].str[8].eq('-'), other=np.nan)
    df['user_uuid'] = df['user_uuid'].mask(~df['user_uuid'].str[13].eq('-'), other=np.nan)
    df['user_uuid'] = df['user_uuid'].mask(~df['user_uuid'].str[18].eq('-'), other=np.nan)
    df['user_uuid'] = df['user_uuid'].mask(~df['user_uuid'].str[23].eq('-'), other=np.nan)
    df['card_number'] = df['card_number'].astype(str)
    df['store_code'] = df['store_code'].str.strip()
    code_check_condition = np.where(df['store_code'].str.startswith('WEB'), 
                     df['store_code'].str[3].eq('-'), 
                     df['store_code'].str[2].eq('-'))
    df = df[code_check_condition]
    df['product_code'] = df['product_code'].mask(~df['product_code'].str[2].eq('-'), other=np.nan)
    df.drop_duplicates(inplace=True)
    df = df.dropna()
    return new_db_connector.upload_to_db(df, 'orders_table')

new_data_cleaner = DataCleaning()

# new_data_cleaner.clean_user_data(users_df)
new_data_cleaner.clean_orders_data(orders_df)

# import pandas as pd
# from sqlalchemy import create_engine
# import psycopg2
# from dotenv import find_dotenv, load_dotenv
# import os
# from dateutil.parser import parse
# from dateutil.parser._parser import ParserError
# import numpy as np
# import data_extraction

# load_dotenv(find_dotenv())

# DATABASE_TYPE = os.environ.get('DATABASE_TYPE')
# DBAPI = os.environ.get('DBAPI')
# HOST = os.environ.get('HOST')
# # USER = os.environ.get('USER')
# USER = 'postgres'
# PASSWORD = os.environ.get('PASSWORD')
# DATABASE = os.environ.get('DATABASE')
# PORT = 5432


# engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
# engine.execution_options(isolation_level='AUTOCOMMIT').connect()

# link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

# df_from_extraction = data_extraction.new_df.retrieve_pdf_data(link)

# products_df = data_extraction.new_df.convert_csv_to_df()

# product_categories = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy']

# removed_categories = ['Still_avaliable', 'Removed']


# class DataCleaning:

#   def clean_card_data(self, df):
#     df.replace(to_replace = 'NULL', value=None, inplace=True)
#     df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
#     df['expiry_date'] = pd.to_datetime(df['expiry_date'], format='%m/%d', errors='coerce')
#     df.dropna(inplace=True, ignore_index=True)
#     df['expiry_date'] = df['expiry_date'].astype(str).str[5:]
#     df['card_number'] = df['card_number'].astype(str).str.strip().str.upper()
#     df['card_provider'] = df['card_provider'].str.strip().str.upper()
#     self.upload_to_db(df)

#   def upload_to_db(self, df):
#     df.to_sql('dim_card_details', engine, if_exists='replace')  
    

#   def clean_store_data(self, filename):
#     df = pd.read_json(filename)
#     df['address'] = df['address'].replace('\n', ' ', regex=True).str.strip().str.upper()
#     df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
#     df = df.drop('lat', axis=1)
#     df['locality'] = df['locality'].str.strip().str.upper()
#     df['store_code'] = df['store_code'].str.strip()
#     df = df[df['store_code'].str[2].eq('-')]
#     df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')
#     df = df.dropna(subset=['staff_numbers'])
#     df['staff_numbers'] = df['staff_numbers'].astype('int32')
#     df['opening_date'] = df['opening_date'].apply(parse)
#     df['opening_date'] = pd.to_datetime(df['opening_date'], infer_datetime_format=True, errors='coerce')
#     df['store_type'] = df['store_type'].str.strip().str.upper()
#     df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
#     df['continent'] = df['continent'].replace('ee', '', regex=True).str.strip().str.upper()
#     df = df.drop('message', axis=1)
#     df.to_sql('dim_store_details', engine, if_exists='replace')

#   def convert_product_weights(self, df):
#     df['temp_weight'] = df['weight']
#     df['temp_weight'] = df['temp_weight'].str.replace('[A-Za-z]', '', regex=True)
#     df['temp_weight'] = pd.to_numeric(df['temp_weight'], errors='coerce')
#     df.loc[(df['weight'].str[-1] == 'g') & (df['weight'].str[-2] != 'k'), 'temp_weight'] = df['temp_weight'] / 1000
#     df.loc[df['weight'].str[-2:] == 'ml', 'temp_weight'] = df['temp_weight'] / 1000
#     df.loc[df['weight'].str[-2:] == 'oz', 'temp_weight'] = df['temp_weight'] * 0.0283495

#     df = df.drop('weight', axis=1)
#     df = df.rename(columns={'temp_weight': 'weight'})
#     self.clean_products_data(df)

#   def parse_date(self, date_column):
#     if pd.isna(date_column):
#       return None
#     try:
#       return parse(date_column)
#     except (ParserError, TypeError):
#       return None
    
#   def impute_product_weights(self, df, products_without_weights):
#     for product in products_without_weights:
#       non_nan_weight = df[(df['product_name'] == product) & (~df['weight'].isna())]['weight'].mean()
#       if pd.notna(non_nan_weight):
#         df.loc[(df['product_name'] == product) & (df['weight'].isna()), 'weight'] = non_nan_weight
#     return df    


#   def clean_products_data(self, df):
#     pd.set_option('display.max_rows', None)
#     df['product_name'] = df['product_name'].str.strip()
#     df = df.rename(columns={'product_price': 'product_price_gbp'})
#     df['product_price_gbp'] = df['product_price_gbp'].str.replace('£', '')
#     df['product_price_gbp'] = pd.to_numeric(df['product_price_gbp'], errors='coerce')
#     df.loc[~df['category'].isin(product_categories), 'category'] = np.nan
#     df['EAN'] = pd.to_numeric(df['EAN'], errors='coerce').astype(str)
#     df['date_added'] = df['date_added'].apply(self.parse_date)
#     df['date_added'] = pd.to_datetime(df['date_added'], infer_datetime_format=True, errors='coerce')
#     df['uuid'] = df['uuid'].mask(~df['uuid'].str[8].eq('-'), other=np.nan)
#     df['uuid'] = df['uuid'].mask(~df['uuid'].str[13].eq('-'), other=np.nan)
#     df['uuid'] = df['uuid'].mask(~df['uuid'].str[18].eq('-'), other=np.nan)
#     df['uuid'] = df['uuid'].mask(~df['uuid'].str[23].eq('-'), other=np.nan)
#     df.loc[~df['removed'].isin(removed_categories), 'removed'] = np.nan
#     df['removed'] = df['removed'].str.replace('avaliable', 'available')
#     df['product_code'] = df['product_code'].mask(~df['product_code'].str[2].eq('-'), other=np.nan)
#     products_without_weights = df[df['weight'].isna()]['product_name']
#     df = self.impute_product_weights(df, products_without_weights)
#     df = df.dropna()
#     df.to_sql('dim_products', engine, if_exists='replace')



# df_to_clean = DataCleaning()

# # df_to_clean.clean_card_data(df_from_extraction)

# # df_to_clean.clean_store_data('stores_data.json')
# df_to_clean.convert_product_weights(products_df)