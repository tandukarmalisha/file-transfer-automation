import os
import pandas as pd
from celery import Celery
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Database Configuration
user=os.getenv("DB_USER")
password=os.getenv("DB_PASSWORD")
host=os.getenv("DB_HOST")
port=os.getenv("DB_PORT")
name=os.getenv("DB_NAME")

DB_PATH = f"postgresql://{user}:{password}@{host}:{port}/{name}"
engine = create_engine(DB_PATH)

app = Celery('etl_system', broker='redis://localhost:6379/0')

@app.task(bind=True, max_retries=5, default_retry_delay=10)
def process_file_etl(self, source_path):
    try:
        # 1. EXTRACT: Read the new Excel file
        new_data = pd.read_excel(source_path)

        # 2. TRANSFORM: Add metadata tracking
        new_data['extracted_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_data['source_filename'] = os.path.basename(source_path)

        # 3. LOAD: Save to Database Table
        # Pandas creates the 'transactions' table automatically here
        new_data.to_sql('transactions', con=engine, if_exists='append', index=False)

        # 4. CLEANUP: Delete the original file after successful DB entry
        if os.path.exists(source_path):
            os.remove(source_path)
        
        return f"Successfully moved {os.path.basename(source_path)} data to Database."

    except Exception as e:
        print(f"Error processing Excel file: {e}")
        # Retries handle cases where the DB might be momentarily locked
        raise self.retry(exc=e)