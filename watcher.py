import time
import os
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from tasks import process_file_etl

# 1. Dynamic Path Setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WATCH_DIRECTORY = os.path.join(BASE_DIR, "incoming_files")
UNWANTED_DIRECTORY = os.path.join(BASE_DIR, "unwanted_files")

# Ensure necessary directories exist
for folder in [WATCH_DIRECTORY, UNWANTED_DIRECTORY]:
    if not os.path.exists(folder):
        os.makedirs(folder)

class ETLHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        
        filename = os.path.basename(event.src_path)
        
        # Filter out temporary Excel owner files (~$)
        if filename.startswith('~$'):
            return

        file_ext = os.path.splitext(filename)[1].lower()
        
        if file_ext == '.xlsx':
            print(f"Excel file detected: {filename}")
            process_file_etl.delay(os.path.abspath(event.src_path))
        else:
            # SEGREGATION: Move unwanted file types to a different folder
            print(f"Non-Excel file detected. Segregating: {filename}")
            # FIXED: Changed UNWANTED_FILES to UNWANTED_DIRECTORY
            dest_path = os.path.join(UNWANTED_DIRECTORY, filename)
            
            try:
                shutil.move(event.src_path, dest_path)
            except Exception as e:
                print(f"Failed to move unwanted file {filename}: {e}")

if __name__ == "__main__":
    event_handler = ETLHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIRECTORY, recursive=False)
    
    print(f"Watcher started!")
    print(f"Monitoring: {WATCH_DIRECTORY}")
    print(f"Segregating unwanted files to: {UNWANTED_DIRECTORY}")
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nWatcher stopped.")
    observer.join()