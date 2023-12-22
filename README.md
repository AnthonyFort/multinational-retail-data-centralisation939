# Multinational Retail Data Centralisation Project

## Introduction

This project was an exercise in data-cleaning, data-analysis, and AWS. It was undertaken as part of my training on the AI Core Government Skills Bootcamp.

The premise is that I work for a multinational sales company whose sales data is currently spread across several sources. My task was to organise that data and bring it into one centralised location.

After organising the data in this way, my subsequent task was to do some data analysis and gain insight into this business's sales history.

### Languages and Tools Used

AWS, Pandas, PGAdmin4, Python, PostgreSQL, SQL 

## File Structure

`main.py` is the file from where the scripts are run. It imports class instances from the following scripts, and executes them in a logical order:

- `data_utils.py` (establishes user credentials and connects both to the original, dispersed databases and to the new, centralised one)
- `data_extraction.py` (provides methods for extracting data from various sources and in different formats, returning, either a pandas dataframe, or a file that can easily be converted into a pandas dataframe)
- `data_cleaning.py` (provides methods for cleaning the data)

Two further text files, while not programming scripts, contain ordered collections of SQL queries:

- `data_queries_record.txt` (performs further cleaning operations on the data after it has been uploaded to the new database)
- `further_data_queries.txt` (provides a list of queries which return information about the cleaned data)

## License Information

This was a project completed as part of the AI Core skills bootcamp. Please contact AI Core for licensing information.