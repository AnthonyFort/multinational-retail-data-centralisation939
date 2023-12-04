import pandas as pd
import data_extraction

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
    print(df.isnull().values.any())
    print(df.isnull().sum())
    print(df['card_number'].head(20))
    
    # print(df['expiry_date'].head(10))
    # print(df.describe())
    # df.info()

df_to_clean = DataCleaning()

df_to_clean.clean_card_data(df_from_extraction)