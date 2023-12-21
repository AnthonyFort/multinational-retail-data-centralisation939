import pandas as pd
import tabula
import boto3
import os
import requests
import json

from functools import reduce
from dotenv import find_dotenv, load_dotenv

from data_utils import DatabaseConnector

s3 = boto3.client('s3')

api_key = os.environ.get('x-api-key')

headers = {
  'Content-Type': 'application/json',
  'x-api-key': api_key
}

card_details_link = os.environ.get('card_details_link')
store_endpoint = os.environ.get('store_endpoint')
store_numbers_url = os.environ.get('store_numbers_url')
filename_for_downloaded_csv = os.environ.get('filename_for_downloaded_csv')
filename_for_dim_date_times = os.environ.get('filename_for_dim_date_times')
sales_url = os.environ.get('sales_url')

class DataExtractor():

  def __init__(self, db_connector):
    self.db_connector = db_connector

  def read_rds_table(self, table_name):
    try:
      with self.db_connector.engine.connect() as conn:
        return pd.read_sql_table(table_name, conn)
    except Exception as e:
      print(e)  

  def retrieve_pdf_data(self, link):
    try:
      dfs = tabula.read_pdf(link, pages='all') # Returns list of dataframes
      df = reduce(lambda left, right: pd.concat((left, right), axis=0, join='outer', ignore_index=True), dfs) # Joins dataframes into single dataframe
      return df    
    except Exception as e:
      print(e)  

  def list_number_of_stores(self, endpoint, header_dict):
    response = requests.get(endpoint, headers=header_dict)
    data = response.json()
    number_of_stores = data['number_stores']
    return number_of_stores   
  
  def retreive_stores_data(self, number_of_stores, store_endpoint):
    json_responses = []
    for n in range( number_of_stores):
      response = requests.get(f'{store_endpoint}{n}', headers=headers)
      data = json.loads(response.text)
      json_responses.append(data)
    with open('stores_data.json', 'w') as f:
      json.dump(json_responses, f)  

  def extract_from_s3(self, bucket, key, filename):
    try:
      s3.download_file(bucket, key, filename)    
    except Exception as e:
      print(e)  

  def convert_csv_to_df(self, filename):
    df = pd.read_csv(filename)
    return df   

  def extract_json(self, url):
    try:
      response = requests.get(url)
      if response.status_code == 200:
        data = response.json()
        with open('dim_date_times', 'w') as f:
          json.dump(data, f)
      else:
        print(response.status_code)  
    except Exception as e:
      print(e) 

db_connector = DatabaseConnector()
db_connector.read_db_creds('db_creds.yaml')
db_connector.init_db_engine()

data_extractor = DataExtractor(db_connector)
user_data_df = data_extractor.read_rds_table('legacy_users')
card_data_df = data_extractor.retrieve_pdf_data(card_details_link)
products_df = data_extractor.convert_csv_to_df('products.csv')
orders_df = data_extractor.read_rds_table('orders_table')

# number_of_stores = data_extractor.list_number_of_stores(store_numbers_url, headers)
# data_extractor.retreive_stores_data(number_of_stores, store_endpoint)
# products_df = data_extractor.extract_from_s3('data-handling-public', 'products.csv')
# data_extractor.extract_json(sales_url)



# import boto3
# import data_utils
# import json
# import os
# import pandas as pd
# import requests
# import tabula

# from dotenv import find_dotenv, load_dotenv
# from functools import reduce

# load_dotenv(find_dotenv())
# s3 = boto3.client('s3')

# api_key = os.environ.get('x-api-key')
# filename_for_downloaded_csv = os.environ.get('filename_for_downloaded_csv')
# sales_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
# store_endpoint =  'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
# store_numbers_url = ' https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

# headers = {
#   'Content-Type': 'application/json',
#   'x-api-key': api_key
# }

# class DataExtractor():

#   def retrieve_pdf_data(self, link):
#     dfs = tabula.read_pdf(link, pages='all') # Returns list of dataframes
#     df = reduce(lambda left, right: pd.concat((left, right), axis=0, join='outer', ignore_index=True), dfs) # Joins dataframes into single dataframe
#     return df
  
#   def list_number_of_stores(self, endpoint, header_dict):
#     response = requests.get(endpoint, headers=header_dict)
#     data = response.json()
#     number_of_stores = data['number_stores']
#     self.retreive_stores_data(number_of_stores, store_endpoint=store_endpoint)

#   def retreive_stores_data(self, number_of_stores, store_endpoint):
#     json_responses = []
#     for n in range( number_of_stores):
#       response = requests.get(f'{store_endpoint}{n}', headers=headers)
#       data = json.loads(response.text)
#       json_responses.append(data)
#     with open('stores_data.json', 'w') as f:
#       json.dump(json_responses, f)  

#   def extract_from_s3(self, bucket, key):
#     s3.download_file(bucket, key, filename_for_downloaded_csv)

  
#   def read_rds_table(self, connector, table_name):
#     engine = connector.read_db_creds('db_creds.yaml')
#     return pd.read_sql_table(table_name, engine)
  
#   def extract_from_s3(self, bucket, key, filename):
#     s3.download_file(bucket, key, filename)

#   def convert_csv_to_df(self):
#     df = pd.read_csv('products.csv')
#     return df

#   def extract_json(self, url):
#     try:
#       response = requests.get(url)
#       if response.status_code == 200:
#         data = response.json()
#         with open('dim_date_times', 'w') as f:
#           json.dump(data, f)
#       else:
#         print(response.status_code)  
#     except Exception as e:
#       print(e)

# new_data_extractor = DataExtractor()

# users_df = new_data_extractor.read_rds_table(data_utils.new_db_connector, 'legacy_users')
# orders_df = new_data_extractor.read_rds_table(data_utils.new_db_connector, 'orders_table')
# new_data_extractor.extract_json(sales_url)



# # new_data_extractor.read_rds_data(data_utils.new_db_connector.read_db_creds('db_creds.yaml', 'legacy_store_details'))
# # new_data_extractor.read_rds_data(data_utils.new_db_connector.read_db_creds('db_creds.yaml', 'orders_table'))


# # new_df = DataExtractor()

# # # new_df.list_number_of_stores(store_numbers_url, headers)

# # # new_df.extract_from_s3('data-handling-public', 'products.csv')

# # new_df.convert_csv_to_df()