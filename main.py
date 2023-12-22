from data_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

import os
from dotenv import find_dotenv, load_dotenv
import boto3

'''
This script is the location from where the various methods in other
scripts will be run in a logical order, first creating the engines,
then extracting the uncleaned data, and finally uploading the cleaned data.
'''

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

if __name__ == '__main__':    
  db_connector = DatabaseConnector()
  db_connector.read_db_creds('db_creds.yaml')
  db_connector.init_db_engine()

  data_extractor = DataExtractor(db_connector)
  user_data_df = data_extractor.read_rds_table('legacy_users')
  card_data_df = data_extractor.retrieve_pdf_data(card_details_link)
  products_df = data_extractor.convert_csv_to_df('products.csv')
  orders_df = data_extractor.read_rds_table('orders_table')

  data_cleaner = DataCleaning(db_connector)
  db_connector.init_upload_engine()

  cleaned_user_data = data_cleaner.clean_user_data(user_data_df)
  cleaned_card_data = data_cleaner.clean_card_data(card_data_df)
  cleaned_store_data = data_cleaner.clean_store_data('stores_data.json')
  cleaned_products_data = data_cleaner.convert_product_weights(products_df)
  cleaned_orders_data = data_cleaner.clean_orders_data(orders_df)
  cleaned_date_times_data = data_cleaner.clean_dim_date_times('dim_date_times')

  db_connector.upload_to_db(cleaned_user_data, 'dim_users')
  db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')
  db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')
  db_connector.upload_to_db(cleaned_products_data, 'dim_products')
  db_connector.upload_to_db(cleaned_orders_data, 'orders_table')
  db_connector.upload_to_db(cleaned_date_times_data, 'dim_date_times')
  