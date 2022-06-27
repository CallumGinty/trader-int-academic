# Enter your MySQL database configuration here and rename file as "database_config.py"

DB_name = 'traderint' # Enter a database name. Used for database creation and connection.

# Configuration for connecting to an existing database. 
config = {
'user': 'ABC',
'password': '123',
'host' : 'localhost',
'database' : DB_name}

# Configuration for new databases, this is run when no database currently exists.
createDBconfig = {
    'user': 'ABC',
    'password': '123',
    'host' : 'localhost'}