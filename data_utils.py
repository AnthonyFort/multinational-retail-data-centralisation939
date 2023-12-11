import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
  
  def read_db_creds(self, creds):
    with open(creds, 'r') as f:
      creds_dict = yaml.safe_load(f)
      return self.init_db_engine(creds_dict)

  def init_db_engine(self, creds):
    engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
    engine.execution_options(isolation_level='AUTOCOMMIT').connect()
    return self.list_db_tables(engine)

  def list_db_tables(self, engine):
    inspection = inspect(engine)
    print(inspection.get_table_names())
    return engine
    

new_db_connector = DatabaseConnector()

# new_db_connector.read_db_creds('db_creds.yaml')