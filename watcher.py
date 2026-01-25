import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from tasks import process_file_etl

# 1. Dynamic Path Setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WATCH_DIRECTORY = os.path.join(BASE_DIR, "incoming_files")

# Ensure the directory exists so the script doesn't crash on startup
if not os.path.exists(WATCH_DIRECTORY):
    os.makedirs(WATCH_DIRECTORY)

class ETLHandler(FileSystemEventHandler):
    def on_created(self, event):
        # 2. Filter for Excel Files Only
        # We only want to trigger the task for .xlsx files
        if event.is_directory:
            return
        
        file_ext = os.path.splitext(event.src_path)[1].lower()
        
        if file_ext == '.xlsx':
            print(f"Excel file detected: {os.path.basename(event.src_path)}")
            
            # Send the absolute path to the Celery Worker
            process_file_etl.delay(os.path.abspath(event.src_path))
        else:
            # Ignore other files (like the .pyc files mentioned in your notes)
            print(f"Skipping non-excel file: {os.path.basename(event.src_path)}")

if __name__ == "__main__":
    event_handler = ETLHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIRECTORY, recursive=False)
    
    print(f"Watcher started! Monitoring: {WATCH_DIRECTORY}...")
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nWatcher stopped.")
    observer.join()