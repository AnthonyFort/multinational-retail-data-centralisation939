import tabula
import pandas as pd
from functools import reduce
import requests
from dotenv import find_dotenv, load_dotenv
import os

load_dotenv(find_dotenv())

store_numbers_url = ' https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
api_key = os.environ.get('x-api-key')

headers = {
  'Content-Type': 'application/json',
  'x-api-key': api_key
}

class DataExtractor():

  def retrieve_pdf_data(self, link):
    dfs = tabula.read_pdf(link, pages='all') # Returns list of dataframes
    df = reduce(lambda left, right: pd.concat((left, right), axis=0, join='outer', ignore_index=True), dfs) # Joins dataframes into single dataframe
    return df
  
  def list_number_of_stores(self, endpoint, header_dict):
    response = requests.get(endpoint, headers=header_dict)
    data = response.json()
    number_of_stores = data['number_stores']
    return number_of_stores

new_df = DataExtractor()

new_df.list_number_of_stores(store_numbers_url, headers)