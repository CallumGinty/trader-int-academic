#to run this code, open terminal and use "python filename.py" (Does not require Python 3.9 unlike run-search.py)
# VERSION 1 Botometer operations have been turned into functions, authentication working
# VERSION 2 imports a list of account IDs from a DB, finds the score, prints back to DB.
# VERSION 3 multi threading working, loop counter working with variable shared across processes, appears problematic.
# VERSION 4 PLANNED: clean up code, improved resillence. proper stop of process when loop limit reached.
# VERSION 5 Working with mysql again, main error was in futures "chunksize" being smaller than the list size. multithreading not working.
# VERSION 6 Get multithreading working again.

from datetime import datetime
import concurrent.futures
import multiprocessing
import sys # used to exit the program upon creating database
import time # used for measuring run time
import botometer #Botometer will only work with tweepy version < 3.10
import mysql.connector # For connecting to dataset

from database_config import config, createDBconfig, DB_name
from api_keys import rapidapi_key, twitter_app_auth

    # CREATE / CONNECT TO DB FILE
print("Welcome to Botscorer, MySQL version")

starttime = int(time.perf_counter()) #start the time counter and convert variable to an interger
##################### Start database connection and cursor #####################
try:
    db = mysql.connector.connect(**config) # Connect to a database with the config set in the parameters
    print("Connection to database", DB_name, "successful.\n")
    cursor = db.cursor() # Instantiate a cursor to work with the specified database
except Exception as DB_con_error: #If there is no database yet, run the script twice. First will create the database then error out. the secondtime will run the actual program.
    print(f"Couldnt create default cursor with error: {DB_con_error}, trying to create the database instead")
    DB_2 = mysql.connector.connect(**CREATE_DB_CONFIG) # Connect to a database with the config set in the parameters
    CURSOR_2 = DB_2.cursor() # Instantiate a cursor to work with the specified database
    print("Setup cursor for database creation.")

    def create_database():
        CURSOR_2.execute("CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8mb4' COLLATE utf8mb4_0900_ai_ci".format(DB_name)) #https://dba.stackexchange.com/questions/76788/create-a-mysql-database-with-charset-utf-8
        print("Database created:", DB_name)
    create_database()     # NOTE: When creating a database, need to remove the database parameter in the "config" dictionary.
    sys.exit("Database created, now exiting.") #exiting here so you run the script twice in case there is no database first.


bom = botometer.Botometer(wait_on_ratelimit=True, rapidapi_key=rapidapi_key, **twitter_app_auth)
print("Twitter and RapidAPI Auth completd.")
########################## Define functions ##########################
def count_rows_tweets():
    sql = ("SELECT COUNT(`id_str`) FROM users")
    cursor.execute(sql)
    result = cursor.fetchall() # Gets the result as a tuple. Can use "cursor.fetchone()" to get a single row.
    sql2 = ("SELECT COUNT(`user_id`) FROM users")
    cursor.execute(sql2)
    result2 = cursor.fetchall() # Gets the result as a tuple. Can use "cursor.fetchone()" to get a single row.
    print("Users scanned:", result[0][0], "Out of total:", result2[0][0])

    # Check a single account by id passed in by the line calling the function
def check_account(UID):
    # cancel process if scans this session is reached.
#    if RUN_COUNTER.value >= 85000:
#        return print ("Loop limit reached, cannot scan:", UID, "\n")
    print("On user ID:", UID) #UID = user id

    # Time stamps for recording the date for each API call, these are stored in the database.
    isotime = datetime.now() # get current time in ISO format
    epoch_time = int(time.time()) # get current time in unix epoch format

    # scan counter
    with RUN_COUNTER.get_lock():
        RUN_COUNTER.value += 1 # as the function is mapped across 4 different processes, each process has their own memory space and hence, their own instance of the global variable "already_scanned". can implement a shared memory here: https://stackoverflow.com/questions/61299918/shared-variable-in-concurrent-futures-processpoolexecutor-python
    # ratelimits = bom.twitter_api.rate_limit_status()
    # searchtweets = RL["resources"]["search"]['/search/tweets']["remaining"]
    # usertimeline = RL["resources"]["statuses"]['/statuses/user_timeline']["remaining"]
    # print ("API calls this session:", RUN_COUNTER.value, "/search/tweets calls remaining:", searchtweets, "/statuses/user_timeline calls remaining:", usertimeline)
    print(f"API calls this session: {RUN_COUNTER.value} out of {scan_size}")

    count_rows_tweets()

    try:
    # Stop the current loop if the row is already populated with data
        cursor.execute("SELECT `id_str` FROM users WHERE user_id = %(iteration)s LIMIT 1", {'iteration': UID}) #fastest was to check -> https://stackoverflow.com/questions/1676551/best-way-to-test-if-a-row-exists-in-a-mysql-table
        currentuser = cursor.fetchall() # first gets the row of the account the loop is on. We check if it has been scanned already and skips if yes.
        print("id_str for currently selected row:", currentuser) # TEST if current loop has an empty frame in the id_str column - unscanned accounts will have this feild empty
        if currentuser != [(None,)]:
            return print("Row already scanned, skipping:", currentuser, "\n")  # breaks current function if above statement is true
        else:
            print("Scanning...")
            pass # pass means "there is no code to execute here" and will continue through the remainder of the loop body.
    except Exception as err:
        print("Couldnt get account list!", err)



    # MAIN API CALL #
    try:
        result = bom.check_account(UID)
        print(f"Here are the results for {UID}:")
        print(result) # print API call results
    except Exception as bom_error:
        print(f"Botometer error: {bom_error} - adding suspended tag to entry.")
        cursor.execute("UPDATE users SET `id_str` = 'suspended' WHERE `user_id` = %(ID)s", {'ID': UID})
        # cursor.commit() # This throws and Error! 'CMySQLCursor' object has no attribute 'commit'
        print("updated cell as 'suspended'.\n")
        return



    # USER INFO - parse indiviudal datapoints from the botometer dictionary, then input them into the database
    majority_lang = result['user']['majority_lang']
    id_str = result['user']['user_data']['id_str']
    SN = result['user']['user_data']['screen_name']
    cursor.execute("""UPDATE users SET
    majority_lang = %(ML)s,
    id_str = %(IDS)s,
    screen_name = %(sn)s,
    date_updated_ISO = %(timeiso)s,
    date_updated_unix = %(timeunix)s
    WHERE user_id = %(UID)s""",
    {'ML': majority_lang, 'IDS': id_str, 'sn': SN, 'timeiso': isotime, 'timeunix': epoch_time, 'UID': UID})
    db.commit()

    # CAP SCORES
    cap_english = result['cap']['english']
    cap_universal = result['cap']['universal']
    cursor.execute("""UPDATE users SET
    cap_english = %(CPE)s,
    cap_universal = %(CPU)s
    WHERE user_id = %(UID)s""",
    {'CPE': cap_english, 'CPU': cap_universal, 'UID': UID})
    db.commit()

    # DISPLAY SCORES
    display_eng_astroturf = result['display_scores']['english']['astroturf']
    display_eng_fakefollower = result['display_scores']['english']['fake_follower']
    display_eng_financial = result['display_scores']['english']['financial']
    display_eng_other = result['display_scores']['english']['other']
    display_eng_overall = result['display_scores']['english']['overall']
    display_eng_selfdeclared = result['display_scores']['english']['self_declared']
    display_eng_spammer = result['display_scores']['english']['spammer']
    cursor.execute("""UPDATE users SET
    display_eng_astroturf = %(display_eng_astroturf)s,
    display_eng_fakefollower = %(display_eng_fakefollower)s,
    display_eng_financial = %(display_eng_financial)s,
    display_eng_other = %(display_eng_other)s,
    display_eng_overall = %(display_eng_overall)s,
    display_eng_selfdeclared = %(display_eng_selfdeclared)s,
    display_eng_spammer = %(display_eng_spammer)s
    WHERE user_id = %(UID)s""",
    {'display_eng_astroturf': display_eng_astroturf, 'display_eng_fakefollower': display_eng_fakefollower,
    'display_eng_financial': display_eng_financial, 'display_eng_other': display_eng_other,
    'display_eng_overall': display_eng_overall, 'display_eng_selfdeclared': display_eng_selfdeclared,
    'display_eng_spammer': display_eng_spammer, 'UID': UID})
    db.commit()

    # DISPLAY SCORES UNIVERSAL
    display_universal_astroturf = result['display_scores']['universal']['astroturf']
    display_universal_fakefollower = result['display_scores']['universal']['fake_follower']
    display_universal_financial = result['display_scores']['universal']['financial']
    display_universal_other = result['display_scores']['universal']['other']
    display_universal_overall = result['display_scores']['universal']['overall']
    display_universal_selfdeclared = result['display_scores']['universal']['self_declared']
    display_universal_spammer = result['display_scores']['universal']['spammer']
    cursor.execute("""UPDATE users SET
    display_universal_astroturf = %(display_universal_astroturf)s,
    display_universal_fakefollower = %(display_universal_fakefollower)s,
    display_universal_financial = %(display_universal_financial)s,
    display_universal_other = %(display_universal_other)s,
    display_universal_overall = %(display_universal_overall)s,
    display_universal_selfdeclared = %(display_universal_selfdeclared)s,
    display_universal_spammer = %(display_universal_spammer)s
    WHERE user_id = %(UID)s""",
    {'display_universal_astroturf': display_universal_astroturf,
    'display_universal_fakefollower': display_universal_fakefollower,
    'display_universal_financial': display_universal_financial,
    'display_universal_other': display_universal_other,
    'display_universal_overall': display_universal_overall,
    'display_universal_selfdeclared': display_universal_selfdeclared,
    'display_universal_spammer': display_universal_spammer,
    'UID': UID})
    db.commit()

    # RAW SCORES
    raw_eng_astroturf = result['raw_scores']['english']['astroturf']
    raw_eng_fakefollower = result['raw_scores']['english']['fake_follower']
    raw_eng_financial = result['raw_scores']['english']['financial']
    raw_eng_other = result['raw_scores']['english']['other']
    raw_eng_overall = result['raw_scores']['english']['overall']
    raw_eng_selfdeclared = result['raw_scores']['english']['self_declared']
    raw_eng_spammer = result['raw_scores']['english']['spammer']
    cursor.execute("""UPDATE users SET
    raw_eng_astroturf = %(raw_eng_astroturf)s,
    raw_eng_fakefollower = %(raw_eng_fakefollower)s,
    raw_eng_financial = %(raw_eng_financial)s,
    raw_eng_other = %(raw_eng_other)s,
    raw_eng_overall = %(raw_eng_overall)s,
    raw_eng_selfdeclared = %(raw_eng_selfdeclared)s,
    raw_eng_spammer = %(raw_eng_spammer)s
    WHERE user_id = %(UID)s""",
    {'raw_eng_astroturf': raw_eng_astroturf, 'raw_eng_fakefollower': raw_eng_fakefollower,
    'raw_eng_financial': raw_eng_financial, 'raw_eng_other': raw_eng_other,
    'raw_eng_overall': raw_eng_overall, 'raw_eng_selfdeclared': raw_eng_selfdeclared,
    'raw_eng_spammer': raw_eng_spammer, 'UID': UID})
    db.commit()
    # RAW SCORES UNIVERSAL
    raw_universal_astroturf = result['raw_scores']['universal']['astroturf']
    raw_universal_fakefollower = result['raw_scores']['universal']['fake_follower']
    raw_universal_financial = result['raw_scores']['universal']['financial']
    raw_universal_other = result['raw_scores']['universal']['other']
    raw_universal_overall = result['raw_scores']['universal']['overall']
    raw_universal_selfdeclared = result['raw_scores']['universal']['self_declared']
    raw_universal_spammer = result['raw_scores']['universal']['spammer']
    cursor.execute("""UPDATE users SET
    raw_universal_astroturf = %(raw_universal_astroturf)s,
    raw_universal_fakefollower = %(raw_universal_fakefollower)s,
    raw_universal_financial = %(raw_universal_financial)s,
    raw_universal_other = %(raw_universal_other)s,
    raw_universal_overall = %(raw_universal_overall)s,
    raw_universal_selfdeclared = %(raw_universal_selfdeclared)s,
    raw_universal_spammer = %(raw_universal_spammer)s
    WHERE user_id = %(UID)s""",
    {'raw_universal_astroturf': raw_universal_astroturf,
    'raw_universal_fakefollower': raw_universal_fakefollower,
    'raw_universal_financial': raw_universal_financial,
    'raw_universal_other': raw_universal_other,
    'raw_universal_overall': raw_universal_overall,
    'raw_universal_selfdeclared': raw_universal_selfdeclared,
    'raw_universal_spammer': raw_universal_spammer,
    'UID': UID})
    db.commit()

    print(f"Results for {UID} committed successfully to database.\n")
    return

# function for creating a global variable to be used across all processes. ->https://stackoverflow.com/questions/61299918/shared-variable-in-concurrent-futures-processpoolexecutor-python
def set_global(args):
    global RUN_COUNTER
    RUN_COUNTER = args
    
#####################
# Start of main body
#####################
starttime = int(time.perf_counter()) #start the time counter and convert variable to an interger
global scan_size
scan_size = 50000 
cursor.execute("SELECT `user_id` FROM users WHERE `id_str` IS NULL ORDER BY RAND() LIMIT %(scan_size)s", {'scan_size': scan_size}) # RANDOM SELECTION
accountstuple = cursor.fetchall() # FIXED by using "conn.row_factory" in line 27 - "fetchall" returns a list of tuples due to the python DB api - https://www.python.org/dev/peps/pep-0249/
db.commit() # Dont know if this is needed
accounts = []
[accounts.append(i[0]) for i in accountstuple] # need to use i[0] to ensure the list is extracted out of the tuple returned by fetchall
#accounts.extend(accountstuple) #also a potential method of extracting items from a tuple

RUN_COUNTER = multiprocessing.Value('i', 0) #I for integer, 0 for starting value. Instantiates a global variable useable across process pools. https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Value

# Multiprocessing version
# with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
# with concurrent.futures.ProcessPoolExecutor(max_workers=4, initializer=set_global, initargs=(RUN_COUNTER,)) as executor:
with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.map(check_account, accounts, timeout=10, chunksize=1000000) #Finished scanning in  56 seconds, or 0 minutes. FOR 8 ACCOUNTS. Note: specifiying default values does not affect the program at all!
#     Note: Map function returns the results in the order they were started. A for loop will return future objects in the order that they finish. 29:22 -> https://www.youtube.com/watch?v=fKl2JW_qrso

## Single thread version for testing
#for x in accounts:
#   check_account(x) # Finished scanning in  191 seconds, or 3 minutes. FOR 8 ACCOUNTS.

finishtime = int(time.perf_counter()) #int the perf_counter float to an integer, easier to read
print("\nFinished scanning in ", (finishtime-starttime),"seconds, or", int((finishtime-starttime)/60), "minutes.")

    # CLOSE DB CONNECTION
db.close()