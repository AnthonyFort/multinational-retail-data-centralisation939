import tabula
import pandas as pd
from pandas import json_normalize
from functools import reduce
import requests
from dotenv import find_dotenv, load_dotenv
import os
import json

load_dotenv(find_dotenv())

store_endpoint =  'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'

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
    self.retreive_stores_data(number_of_stores, store_endpoint=store_endpoint)
  
  def retreive_stores_data(self, number_of_stores, store_endpoint):
    json_responses = []
    for n in range( number_of_stores):
      response = requests.get(f'{store_endpoint}{n}', headers=headers)
      data = json.loads(response.text)
      json_responses.append(data)
    df = pd.DataFrame(json_responses)
    print(df)  

new_df = DataExtractor()

new_df.list_number_of_stores(store_numbers_url, headers)