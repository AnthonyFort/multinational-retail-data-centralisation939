import tabula
import pandas as pd
from functools import reduce

link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'

class DataExtractor():

  def retrieve_pdf_data(self, link):
    dfs = tabula.read_pdf(link, pages='all') # Returns list of dataframes
    df = reduce(lambda left, right: pd.concat((left, right), axis=0, join='outer'), dfs) # Joins dataframes into single dataframe
    print(df)

new_df = DataExtractor()
new_df.retrieve_pdf_data(link)