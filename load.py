import os
import datetime
import pandas as pd
from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from config import CLEAN_DIR, ISSUES_DIR
from dotenv import load_dotenv

class LoadData:
    def __init__(self):
        load_dotenv('pipeline.env')
        self.sql_user = os.getenv('DB_USER')
        self.sql_pass = os.getenv('DB_PASS')
        self.sql_db = os.getenv('DB_Name')
        self.sql_host = os.getenv('DB_HOST')
        self.engine = None

    def _log_issue(self, message):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with open(os.path.join(ISSUES_DIR, 'pipeline_error_log.txt'), 'a', encoding = 'UTF-8') as issue_log:
            issue_log.write(f'{timestamp}: {message}\n')
    
    def connect_db(self):
        connection_string = (
            f"mssql+pyodbc://{self.sql_user}:{self.sql_pass}"
            f"@{self.sql_host}/{self.sql_db}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
            "&Trusted_Connection=no"
        )

        self.engine = create_engine(
            connection_string,
            pool_pre_ping = True,
            future = True
        )

        try:
            with self.engine.connect() as conn:
                db_name = conn.execute(text('SELECT DB_NAME()')).scalar()
            
                if db_name != self.sql_db:
                    self._log_issue(f'Connected to wrong database or database does not exist.')
                    raise ConnectionError('Connected to wrong database or database does not exist.')
            
                conn.execute(text('SELECT 1 FROM sys.tables')).first()

                login = conn.execute(text('SELECT ORIGINAL_LOGIN()')).scalar()

                print(f'Connected successfully as {login} to database {db_name}')

        except OperationalError as e:
            self._log_issue(f'Failed to connect to the database: {e}')
            raise ConnectionError('Failed to connect to SQL Server!') from e
        
loader = LoadData()
loader.connect_db()