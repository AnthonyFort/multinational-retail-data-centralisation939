import pandas as pd
import tabula
import boto3
import os
import requests
import json

from functools import reduce
from dotenv import find_dotenv, load_dotenv

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
  '''
  This class includes the methods that can be used to extract tables
  from various sources and in various formats (eg., aws bucket, csv, json, pdf, rds table).
  '''
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

# number_of_stores = data_extractor.list_number_of_stores(store_numbers_url, headers)
# data_extractor.retreive_stores_data(number_of_stores, store_endpoint)
# data_extractor.extract_json(sales_url)
