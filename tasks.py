import os
import pandas as pd
from celery import Celery
from datetime import datetime

# Setup Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WATCH_DIRECTORY = os.path.join(BASE_DIR, "incoming_files")
# We change the extension to .xlsx
MASTER_FILE = os.path.join(BASE_DIR, "master_data_log.xlsx")

app = Celery('etl_system', broker='redis://localhost:6379/0')

@app.task(bind=True, max_retries=5, default_retry_delay=10)
def process_file_etl(self, source_path):
    try:
        # 1. EXTRACT: Read the new Excel file
        # This handles the dynamic filename passed by the watcher
        new_data = pd.read_excel(source_path)

        # 2. TRANSFORM: Add metadata so you know where the data came from
        new_data['extracted_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_data['source_filename'] = os.path.basename(source_path)

        # 3. LOAD: Append to the Master Excel file
        if os.path.exists(MASTER_FILE):
            # Load existing data and combine with new data
            existing_master = pd.read_excel(MASTER_FILE)
            updated_master = pd.concat([existing_master, new_data], ignore_index=True)
        else:
            # If master doesn't exist yet, this is the first entry
            updated_master = new_data

        # Save back to the master path
        updated_master.to_excel(MASTER_FILE, index=False)

        # 4. CLEANUP: Delete the original dynamic file A
        os.remove(source_path)
        
        return f"Successfully merged Excel rows from {os.path.basename(source_path)}"

    except Exception as e:
        print(f"Error processing Excel file: {e}")
        # Retries are great for Excel because files are often "locked" by other users
        raise self.retry(exc=e)