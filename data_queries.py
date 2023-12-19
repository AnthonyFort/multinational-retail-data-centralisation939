from sqlalchemy import create_engine, inspect, text
from data_cleaning import engine

# engine.execution_options(isolation_level='AUTOCOMMIT').connect()

inspector = inspect(engine)
# print(inspector.get_table_names())

with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as connection:
  # connection.execute(text("ALTER TABLE orders_table ALTER date_uuid TYPE uuid USING date_uuid::uuid;"))
  # connection.execute(text("ALTER TABLE orders_table ALTER user_uuid TYPE uuid USING user_uuid::uuid;"))
  # connection.execute(text("ALTER TABLE orders_table ALTER card_number TYPE varchar(19) USING card_number::varchar(19);"))
  # connection.execute(text("ALTER TABLE orders_table ALTER store_code TYPE varchar(12) USING store_code::varchar(12);"))
  # connection.execute(text("ALTER TABLE orders_table ALTER product_code TYPE varchar(11) USING card_number::varchar(11);"))
  # connection.execute(text("ALTER TABLE orders_table ALTER product_quantity TYPE smallint USING product_quantity::smallint;"))
  # connection.execute(text("ALTER TABLE dim_users ALTER first_name TYPE varchar(255) USING first_name::varchar(255);"))
  # connection.execute(text("ALTER TABLE dim_users ALTER last_name TYPE varchar(255) USING last_name::varchar(255);"))
  # connection.execute(text("ALTER TABLE dim_users ALTER date_of_birth TYPE date USING date_of_birth::date;"))
  # connection.execute(text("ALTER TABLE dim_users ALTER country_code TYPE varchar(2) USING country_code::varchar(2);"))
  # connection.execute(text("ALTER TABLE dim_users ALTER user_uuid TYPE uuid USING user_uuid::uuid;"))
  # connection.execute(text("ALTER TABLE dim_users ALTER join_date TYPE date USING join_date::date;"))
  # connection.execute(text("ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(14);"))
  # connection.execute(text("UPDATE dim_products SET weight_class = CASE WHEN weight < 2 THEN 'Light' WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized' WHEN weight >= 40 AND weight < 140 THEN 'Heavy' ELSE 'Truck_Required' END;"))
  # connection.execute('ALTER TABLE dim_products RENAME COLUMN "EAN" TO ean;')
  # connection.execute("UPDATE dim_products SET ean = CASE WHEN LENGTH(CAST(SPLIT_PART(ean, '.', 1) AS TEXT)) = 13 AND CAST(SPLIT_PART(ean, '.', 1) AS TEXT) ~ '^\d+$' THEN CAST(SPLIT_PART(ean, '.', 1) AS BIGINT) ELSE NULL END;")
  # connection.execute("ALTER TABLE dim_products ALTER ean TYPE varchar(13) USING ean::varchar(13);")
  # connection.execute(text("ALTER TABLE dim_products ALTER uuid TYPE uuid USING uuid::uuid;"))
  # connection.execute("ALTER TABLE dim_products ADD COLUMN still_available BOOL;")
  # connection.execute("UPDATE dim_products SET still_available = CASE WHEN removed = 'Still_available' THEN True WHEN removed = 'Removed' THEN False ELSE NULL END;")
  # connection.execute("ALTER TABLE dim_products DROP COLUMN IF EXISTS removed")
  # connection.execute("ALTER TABLE dim_date_times ALTER month TYPE varchar(2) USING month::varchar(2)")
  # connection.execute("ALTER TABLE dim_date_times ALTER year TYPE varchar(4) USING year::varchar(4)")
  # connection.execute("ALTER TABLE dim_date_times ALTER day TYPE varchar(2) USING day::varchar(2)")
  # connection.execute("ALTER TABLE dim_date_times ALTER date_uuid TYPE uuid USING date_uuid::uuid")
  # connection.execute("ALTER TABLE dim_date_times ALTER time_period TYPE varchar(10) USING time_period::varchar(10)")

  # connection.execute(text("ALTER TABLE dim_date_times ALTER date_uuid TYPE uuid USING date_uuid::uuid;"))

  # connection.execute(text("UPDATE dim_card_details SET card_number = CASE WHEN card_number ~ '[^0-9]' THEN NULL ELSE card_number END;"))
  # connection.execute(text("ALTER TABLE dim_card_details ALTER card_number TYPE varchar(19) USING card_number::varchar(19)"))
  # connection.execute(text("ALTER TABLE dim_card_details ALTER expiry_date TYPE varchar(5) USING expiry_date::varchar(5)"))

  # connection.execute(text("ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid)"))
  # connection.execute(text("ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid)"))
  # connection.execute(text("ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number)"))
  # connection.execute(text("ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code)"))
  # connection.execute(text("ALTER TABLE dim_products ADD PRIMARY KEY (product_code)"))


  # connection.execute("ALTER TABLE orders_table ADD CONSTRAINT fk_users FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid)")
  # connection.execute("ALTER TABLE orders_table ADD CONSTRAINT fk_date_times FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid)")

  connection.execute("ALTER TABLE orders_table ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number)")
  # connection.execute("ALTER TABLE orders_table ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code)")
  # connection.execute("ALTER TABLE orders_table ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code)")


  

  



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