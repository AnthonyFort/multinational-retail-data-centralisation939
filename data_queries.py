from sqlalchemy import create_engine, inspect, text
from data_cleaning import engine

# engine.execution_options(isolation_level='AUTOCOMMIT').connect()

inspector = inspect(engine)
# print(inspector.get_table_names())

with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as connection:
  connection.execute(text("ALTER TABLE orders_table ALTER date_uuid TYPE uuid USING date_uuid::uuid;"))
  connection.execute(text("ALTER TABLE orders_table ALTER user_uuid TYPE uuid USING user_uuid::uuid;"))
  connection.execute(text("ALTER TABLE orders_table ALTER card_number TYPE varchar(19) USING card_number::varchar(19);"))
  connection.execute(text("ALTER TABLE orders_table ALTER store_code TYPE varchar(12) USING store_code::varchar(12);"))
  connection.execute(text("ALTER TABLE orders_table ALTER product_code TYPE varchar(11) USING card_number::varchar(11);"))
  connection.execute(text("ALTER TABLE orders_table ALTER product_quantity TYPE smallint USING product_quantity::smallint;"))
  connection.execute(text("ALTER TABLE dim_users ALTER first_name TYPE varchar(255) USING first_name::varchar(255);"))
  connection.execute(text("ALTER TABLE dim_users ALTER last_name TYPE varchar(255) USING last_name::varchar(255);"))
  connection.execute(text("ALTER TABLE dim_users ALTER date_of_birth TYPE date USING date_of_birth::date;"))
  connection.execute(text("ALTER TABLE dim_users ALTER country_code TYPE varchar(2) USING country_code::varchar(2);"))
  connection.execute(text("ALTER TABLE dim_users ALTER user_uuid TYPE uuid USING user_uuid::uuid;"))
  connection.execute(text("ALTER TABLE dim_users ALTER join_date TYPE date USING join_date::date;"))


  



# SELECT
# 	product_code,
# 	length(product_code)
# FROM
# 	orders_table
# GROUP BY
# 	product_code
# ORDER BY
# 	length(product_code) desc
# LIMIT
# 	1;