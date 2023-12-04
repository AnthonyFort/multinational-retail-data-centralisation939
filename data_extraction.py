import tabula
import pandas as pd
from functools import reduce

class DataExtractor():

  def retrieve_pdf_data(self, link):
    dfs = tabula.read_pdf(link, pages='all') # Returns list of dataframes
    df = reduce(lambda left, right: pd.concat((left, right), axis=0, join='outer', ignore_index=True), dfs) # Joins dataframes into single dataframe
    return df

new_df = DataExtractor()