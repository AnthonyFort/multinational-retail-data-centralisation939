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