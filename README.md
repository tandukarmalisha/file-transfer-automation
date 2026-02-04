The libraries/modules mentioned in requirement file needs to be installed 
using venv is a good practise method for libraries installations(prevents version issues)

To run this automation python script, run following commands:
redis-server    #(requires redis to be installed ahead)
celery -A yourscriptname.py worker --loglevel=info
python yourwatcherscript.py

in .env file, enter db credentials:
DB_USER = yourusername
DB_PASSWORD = yourpassword
DB_HOST = localhost
DB_PORT = 5432(default)
DB_NAME = yourdbname
