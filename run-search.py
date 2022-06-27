# To run this code, open terminal and use "python3.9 filename.py"
# previous versions were named 'mysql-v3.py' (version 3 and below)
# To fixed text encoding issues in MySQL, follow this guide: https://sebhastian.com/mysql-incorrect-string-value/ OR USE: ALTER TABLE tweepy_cashtags_main CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

# Version 1: Setup basics of mysql server in python
# Version 2: Integrate datascraping again (basic)
# Version 3: Cleanup and back to normal operation. Copies from temp table into main table. All fields are saved.
# Version 4: Added cleaning operations, extract users to another table.

import mysql.connector # For connecting to dataset
from mysql.connector import errorcode # Used in error checking on table creation.
import tweepy
import sys # Needed for sys.exit() method
import pandas
import time #for the time.sleep() function, used to create a delay between twint searches. Also using it to track compute times
from database_config import config, createDBconfig, DB_name
from api_keys import apikey, apikeysecret, access_token, access_token_secret

##################### Set parameters and start counters #####################
print("Welcome to TWEEPY ASX Scraper, MySQL version")
loop = 0 # start loop counter
starttime = int(time.perf_counter()) #start the time counter and convert variable to an interger

##################### Start database connection and cursor #####################
try:
    db = mysql.connector.connect(**config) # Connect to a database with the config set in the parameters
    print ("Connection to database", DB_name, "successful.\n")
    cursor = db.cursor() # Instantiate a cursor to work with the specified database
except: #If there is no database yet, run the script twice. First will create the database then error out. the secondtime will run the actual program.
    print ("Couldnt create default cursor, trying to create the database instead")
    db2 = mysql.connector.connect(**createDBconfig) # Connect to a database with the config set in the parameters
    cursor2 = db2.cursor() # Instantiate a cursor to work with the specified database
    print ("Setup cursor for database creation.")

    def create_database():
        cursor2.execute("CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8mb4' COLLATE utf8mb4_0900_ai_ci".format(DB_name)) #https://dba.stackexchange.com/questions/76788/create-a-mysql-database-with-charset-utf-8
        print ("Database created:", DB_name)

    create_database()     # NOTE: When creating a database, need to remove the database parameter in the "config" dictionary.
    sys.exit("Database created, now exiting.") #exiting here so you run the script twice in case there is no database first.

################ Start Twitter connection ####################
auth = tweepy.OAuthHandler(apikey, apikeysecret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

################ Functions ####################
def create_tables():
    cursor.execute ("USE {}".format(DB_name))
    try:        # Create the temp table first (no reason)
        cursor.execute("""CREATE TABLE `tweepy_cashtags_temp` (
        `tweet_id` BIGINT NOT NULL,
        `tweet_id_str` varchar(255),
        `tweet_text_short` text,
        `created_at` varchar(255),
        `derived_URL` varchar(255),
        `source` varchar(255),
        `truncated` varchar(255),
        `in_reply_to_screen_name` varchar(255),
        `in_reply_to_status_id` varchar(255),
        `in_reply_to_user_id` varchar(255),
        `is_quote_status` varchar(255),
        `retweet_count` varchar(255),
        `tweet_language` varchar(255),
        `user_id` BIGINT,
        `user_id_str` varchar(255),
        `name` varchar(255),
        `screen_name` varchar(255),
        `location` varchar(255),
        `derived_account_url` varchar(255),
        `profile_link` varchar(255),
        `user_description` varchar(255),
        `protected` varchar(255),
        `verified` varchar(255),
        `followers_count` BIGINT,
        `friends_count` BIGINT,
        `listed_count` BIGINT,
        `favourites_count` BIGINT,
        `statuses_count` BIGINT,
        `date_user_created` varchar(255),
        `user_utc_offset` varchar(255),
        `user_time_zone` varchar(255),
        `geo_enabled` varchar(255),
        `user_lang` varchar(255),
        `contributors_enabled` varchar(255),
        `is_translator` varchar(255),
        `default_profile` varchar(255),
        `default_profile_image` varchar(255),
        `favorite_count` BIGINT,
        `parsed_user_mentions_id` varchar(255),
        `parsed_user_mentions_screen_name` varchar(255),
        `possibly_sensitive` varchar(255),
        `url_in_tweet` varchar(255),
        `hashtags` varchar(255),
        `cashtags` varchar(255),
        `tweepy_search_term` varchar(255),
        PRIMARY KEY (`tweet_id`)
        ) ENGINE = InnoDB
        """)
        print ("Created table: 'tweepy_cashtags_temp' successfully!")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print ("Table 'tweepy_cashtags_temp' already exists.\n")
        else:
            print (err.msg)

        # Create the main cashtag table
    try:
        cursor.execute("""CREATE TABLE `tweepy_cashtags_main` (
        `tweet_id` BIGINT NOT NULL,
        `tweet_id_str` varchar(255),
        `tweet_text_short` text,
        `created_at` varchar(255),
        `derived_URL` varchar(255),
        `source` varchar(255),
        `truncated` varchar(255),
        `in_reply_to_screen_name` varchar(255),
        `in_reply_to_status_id` varchar(255),
        `in_reply_to_user_id` varchar(255),
        `is_quote_status` varchar(255),
        `retweet_count` varchar(255),
        `tweet_language` varchar(255),
        `user_id` BIGINT,
        `user_id_str` varchar(255),
        `name` varchar(255),
        `screen_name` varchar(255),
        `location` varchar(255),
        `derived_account_url` varchar(255),
        `profile_link` varchar(255),
        `user_description` varchar(255),
        `protected` varchar(255),
        `verified` varchar(255),
        `followers_count` BIGINT,
        `friends_count` BIGINT,
        `listed_count` BIGINT,
        `favourites_count` BIGINT,
        `statuses_count` BIGINT,
        `date_user_created` varchar(255),
        `user_utc_offset` varchar(255),
        `user_time_zone` varchar(255),
        `geo_enabled` varchar(255),
        `user_lang` varchar(255),
        `contributors_enabled` varchar(255),
        `is_translator` varchar(255),
        `default_profile` varchar(255),
        `default_profile_image` varchar(255),
        `favorite_count` BIGINT,
        `parsed_user_mentions_id` varchar(255),
        `parsed_user_mentions_screen_name` varchar(255),
        `possibly_sensitive` varchar(255),
        `url_in_tweet` varchar(255),
        `hashtags` varchar(255),
        `cashtags` varchar(255),
        `tweepy_search_term` varchar(255),
        PRIMARY KEY (`tweet_id`)) ENGINE = InnoDB """)
        print("Table 'tweept_cashtags_main' created successfully!")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print ("Table 'tweepy_cashtags_main' already exists. \n")
        else:
            print (err.msg)
    
def setup_user_table():
    try:
        cursor.execute("""CREATE TABLE `users` (
	`user_id` BIGINT UNIQUE NOT NULL, 
	`id_str` varchar(255), 
	`screen_name` varchar(255), 
    `majority_lang` varchar(255), 
	`date_updated_ISO` varchar(255),
	`date_updated_unix` BIGINT,
	`cap_english` FLOAT(12), 
	`cap_universal` FLOAT(12), 
	`display_eng_astroturf` FLOAT(12), 
	`display_eng_fakefollower` FLOAT(12), 
	`display_eng_financial` FLOAT(12), 
	`display_eng_other` FLOAT(12),
	`display_eng_overall` FLOAT(12),
	`display_eng_selfdeclared` FLOAT(12), 
	`display_eng_spammer` FLOAT(12), 
	`display_universal_astroturf` FLOAT(12), 
	`display_universal_fakefollower` FLOAT(12), 
	`display_universal_financial` FLOAT(12), 
	`display_universal_other` FLOAT(12),
	`display_universal_overall` FLOAT(12),
	`display_universal_selfdeclared` FLOAT(12), 
	`display_universal_spammer` FLOAT(12), 
	`raw_eng_astroturf` FLOAT(12), 
	`raw_eng_fakefollower` FLOAT(12), 
	`raw_eng_financial` FLOAT(12), 
	`raw_eng_other` FLOAT(12),
	`raw_eng_overall` FLOAT(12),
	`raw_eng_selfdeclared` FLOAT(12), 
	`raw_eng_spammer` FLOAT(12), 
	`raw_universal_astroturf` FLOAT(12), 
	`raw_universal_fakefollower` FLOAT(12), 
	`raw_universal_financial` FLOAT(12), 
	`raw_universal_other` FLOAT(12),
	`raw_universal_overall` FLOAT(12),
	`raw_universal_selfdeclared` FLOAT(12), 
	`raw_universal_spammer` FLOAT(12),
	`botolite_bs` FLOAT(12),
    PRIMARY KEY (`user_id`)
    ) Engine = InnoDB DEFAULT CHARSET=utf8mb4
    """)
        print("Added a table.\n")
    except Exception as e:
        print (e,"\n") 

def tweepy1_search(ST):
    tweets_found = 0
    
    for result2 in api.search(q=ST, result_type="recent", count=300, lang="en"):
        
        #Data dictionary: https://developer.twitter.com/en/docs/twitter-api/v1/data-dictionary/object-model/tweet
        # Details on possible parameters: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets
        #NOTE: This v1.1 api gets a "sample" of tweets from the past 7 days, upgrade API to get a "complete" list of tweets - https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/overview
        #NOTE: To get results, it must be a loop as the 'SearchResults' object is a group of objects.

        # List comprehension values and potentially N/A returns. calculation of potentially N/A variable needs to be outside of the dict to enable use of the try:-except: method.
        hashes = [] # Using same list comprehension method as used in cashtag parsing further down. Need to use a list to use the append method on the next line.
        [hashes.append(j["text"]) for j in result2.entities["hashtags"]]
        hashes = str(hashes) #Convert from a list into a string to work with mySQL.

        cash = [] # Using a list comprehension to parse out dictionary values, out of a list of dictionaries (single dictionary if only one cashtag). NOT FUN. Need to use a list to use the append method on the next line.
        [cash.append(i["text"]) for i in result2.entities["symbols"]] #loops through each item of the dictionary symbols, extracts the text field (which are cashtags), then adds it to a predefined list.
        cash = str(cash)
        #to build this function, first indexing was tried, then 'for-loops', then list comprehension.
        try: #only gets the first user mentioned
            parsed_user_mentions_id = result2.entities["user_mentions"][0]["id"]
            parsed_user_mentions_screen_name = result2.entities["user_mentions"][0]["screen_name"]
        except:
            parsed_user_mentions_id = ""
            parsed_user_mentions_screen_name = ""

        try:
            possibly_sensitive = result2.possibly_sensitive
        except:
            possibly_sensitive = ""

        try: #only gets the first url mentioned
            url_in_tweet = result2.entities["urls"][0]["expanded_url"] #references an object, with a sub-object (entities), which references an array (urls), then a place in a list (0), then a dict (url).
        except:
            url_in_tweet = ""

        df_dict = {"tweet_id": result2.id,
               "tweet_id_str": result2.id_str,
               "tweet_text_short": result2.text,
               "created_at": result2.created_at,
               "derived_URL": "https://www.twitter.com/"+result2.user.screen_name+"/status/"+result2.id_str,
               "source": result2.source,
               "truncated": result2.truncated,
               "in_reply_to_screen_name": result2.in_reply_to_screen_name,
               "in_reply_to_status_id": result2.in_reply_to_status_id,
               "in_reply_to_user_id": result2.in_reply_to_user_id,
               "is_quote_status": result2.is_quote_status,
               "retweet_count": result2.retweet_count,
               "tweet_language": result2.lang,
               "user_id": result2.user.id,
               "user_id_str": result2.user.id_str,
               "name": result2.user.name,
               "screen_name": result2.user.screen_name,
               "location": result2.user.location,
               "derived_account_url": "https://www.twitter.com/"+result2.user.screen_name,
               "profile_link": result2.user.url,
               "user_description": result2.user.description,
               "protected": result2.user.protected,
               "verified": result2.user.verified,
               "followers_count": result2.user.followers_count,
               "friends_count": result2.user.friends_count,
               "listed_count": result2.user.listed_count,
               "favourites_count": result2.user.favourites_count,
               "statuses_count": result2.user.statuses_count,
               "date_user_created": result2.user.created_at,
               "user_utc_offset": result2.user.utc_offset,
               "user_time_zone": result2.user.time_zone,
               "geo_enabled": result2.user.geo_enabled,
               "user_lang": result2.user.lang,
               "contributors_enabled": result2.user.contributors_enabled,
               "is_translator": result2.user.is_translator,
               "default_profile": result2.user.default_profile,
               "default_profile_image": result2.user.default_profile_image,
               "favorite_count": result2.favorite_count,
               "parsed_user_mentions_id" : parsed_user_mentions_id,
               "parsed_user_mentions_screen_name": parsed_user_mentions_screen_name,
               "possibly_sensitive" : possibly_sensitive,
               "url_in_tweet" : url_in_tweet,
               "cashtags" : cash,
               "hashtags" : hashes,
               "tweepy_search_term" : ST}
                 
        sql = """INSERT INTO `tweepy_cashtags_temp` (
            `tweet_id`,
            `tweet_id_str`,
            `tweet_text_short`,
            `created_at`,
            `derived_URL`,
            `source`,
            `truncated`,
            `in_reply_to_screen_name`,
            `in_reply_to_status_id`,
            `in_reply_to_user_id`,
            `is_quote_status`,
            `retweet_count`,
            `tweet_language`,
            `user_id`,
            `user_id_str`,
            `name`,
            `screen_name`,
            `location`,
            `derived_account_url`,
            `profile_link`,
            `user_description`,
            `protected`,
            `verified`,
            `followers_count`,
            `friends_count`,
            `listed_count`,
            `favourites_count`,
            `statuses_count`,
            `date_user_created`,
            `user_utc_offset`,
            `user_time_zone`,
            `geo_enabled`,
            `user_lang`,
            `contributors_enabled`,
            `is_translator`,
            `default_profile`,
            `default_profile_image`,
            `favorite_count`,
            `parsed_user_mentions_id`,
            `parsed_user_mentions_screen_name`,
            `possibly_sensitive`,
            `url_in_tweet`,
            `cashtags`,
            `hashtags`,
            `tweepy_search_term`
            )
            VALUES (
            %(tweet_id)s,
            %(tweet_id_str)s,
            %(tweet_text_short)s,
            %(created_at)s,
            %(derived_URL)s,
            %(source)s,
            %(truncated)s,
            %(in_reply_to_screen_name)s,
            %(in_reply_to_status_id)s,
            %(in_reply_to_user_id)s,
            %(is_quote_status)s,
            %(retweet_count)s,
            %(tweet_language)s,
            %(user_id)s,
            %(user_id_str)s,
            %(name)s,
            %(screen_name)s,
            %(location)s,
            %(derived_account_url)s,
            %(profile_link)s,
            %(user_description)s,
            %(protected)s,
            %(verified)s,
            %(followers_count)s,
            %(friends_count)s,
            %(listed_count)s,
            %(favourites_count)s,
            %(statuses_count)s,
            %(date_user_created)s,
            %(user_utc_offset)s,
            %(user_time_zone)s,
            %(geo_enabled)s,
            %(user_lang)s,
            %(contributors_enabled)s,
            %(is_translator)s,
            %(default_profile)s,
            %(default_profile_image)s,
            %(favorite_count)s,
            %(parsed_user_mentions_id)s,
            %(parsed_user_mentions_screen_name)s,
            %(possibly_sensitive)s,
            %(url_in_tweet)s,
            %(cashtags)s,
            %(hashtags)s,
            %(tweepy_search_term)s
            );"""


        # Data needs to be in the following dictionary format for mySQL
        data = {
            'tweet_id':df_dict["tweet_id"],
            'tweet_id_str':df_dict["tweet_id_str"],
            'tweet_text_short':df_dict["tweet_text_short"],
            'created_at':df_dict["created_at"],
            'derived_URL':df_dict["derived_URL"],
            'source':df_dict["source"],
            'truncated':df_dict["truncated"],
            'in_reply_to_screen_name':df_dict["in_reply_to_screen_name"],
            'in_reply_to_status_id':df_dict["in_reply_to_status_id"],
            'in_reply_to_user_id':df_dict["in_reply_to_user_id"],
            'is_quote_status':df_dict["is_quote_status"],
            'retweet_count':df_dict["retweet_count"],
            'tweet_language':df_dict["tweet_language"],
            'user_id':df_dict["user_id"],
            'user_id_str':df_dict["user_id_str"],
            'name':df_dict["name"],
            'screen_name':df_dict["screen_name"],
            'location':df_dict["location"],
            'derived_account_url':df_dict["derived_account_url"],
            'profile_link':df_dict["profile_link"],
            'user_description':df_dict["user_description"],
            'protected':df_dict["protected"],
            'verified':df_dict["verified"],
            'followers_count':df_dict["followers_count"],
            'friends_count':df_dict["friends_count"],
            'listed_count':df_dict["listed_count"],
            'favourites_count':df_dict["favourites_count"],
            'statuses_count':df_dict["statuses_count"],
            'date_user_created':df_dict["date_user_created"],
            'user_utc_offset':df_dict["user_utc_offset"],
            'user_time_zone':df_dict["user_time_zone"],
            'geo_enabled':df_dict["geo_enabled"],
            'user_lang':df_dict["user_lang"],
            'contributors_enabled':df_dict["contributors_enabled"],
            'is_translator':df_dict["is_translator"],
            'default_profile':df_dict["default_profile"],
            'default_profile_image':df_dict["default_profile_image"],
            'favorite_count':df_dict["favorite_count"],
            'parsed_user_mentions_id':df_dict["parsed_user_mentions_id"],
            'parsed_user_mentions_screen_name':df_dict["parsed_user_mentions_screen_name"],
            'possibly_sensitive':df_dict["possibly_sensitive"],
            'url_in_tweet':df_dict["url_in_tweet"],
            'cashtags':df_dict["cashtags"],
            'hashtags':df_dict["hashtags"],
            'tweepy_search_term':df_dict["tweepy_search_term"]
                }
        
        try:
            cursor.execute(sql, data)
#            print ("Row added!")
            tweets_found += 1
        except Exception as e:
            print (e)
        
    db.commit()
    print("Tweets found:", tweets_found)
    
    # Clear out the Temp table from retweets before merging with the main table. 
    cursor.execute("DELETE FROM tweepy_cashtags_temp WHERE `tweet_text_short` LIKE 'RT @%'")

    # print ("Done with:", ST)
    merge_sql = """INSERT IGNORE tweepy_cashtags_main
    SELECT * FROM tweepy_cashtags_temp
    """
    # with db: # Combine temp table and main table
    cursor.execute(merge_sql,  params=None)
    print ("Saved results from", ST, "to main table successful.")

# Delete the temp table to make way for a fresh extract
    clear_sql = """DELETE FROM tweepy_cashtags_temp"""
    cursor.execute(clear_sql,  params=None)
#    print ("Deleted temp table for:", ST)
def delete_RTs():
    try:
        cursor.execute("DELETE FROM tweepy_cashtags_main WHERE `tweet_text_short` LIKE 'RT @%'")
        db.commit()
        print ("Deleted retweets.")
    except Exception as e:
        print("Error with delete_RTs:", e)

def populate_users_table():
    try:
        cursor.execute("INSERT IGNORE INTO users (`user_id`) SELECT DISTINCT `user_id` FROM tweepy_cashtags_main")
        count = cursor.rowcount
        print ("Users table updated.\n")
        print ("Affected rows:", count)
        db.commit()
    except Exception as e:
        print("Populate users table error:", e)

def count_rows_users():
	sql = ("SELECT COUNT(`user_id`) FROM users")
	cursor.execute(sql)
	result = cursor.fetchall() # Gets the result as a tuple. Can use "cursor.fetchone()" to get a single row.
	print ("The number of unique accounts are:", result[0][0],"\n") 

def count_rows_tweets():
	sql = ("SELECT COUNT(`tweet_id`) FROM tweepy_cashtags_main")
	cursor.execute(sql)
	result = cursor.fetchall() # Gets the result as a tuple. Can use "cursor.fetchone()" to get a single row.
	print ("The number of Tweets saved is:", result[0][0]) 
     
##################### Main Block #####################
stocks_df = pandas.read_csv(r'ISIN-cleaned.csv') # Import the stock list and set it as the search terms
ST = stocks_df.loc[:, 'cashtags'] #make the 'cashtags' column into a list # ST is shorthand for "search term".
#print (ST) # Uncheck for debugging.
# ST = ["$QAN"] # Uncheck for debugging. NOTE: This MUST be a tuple or a list. If left as a string it will loop over each letter.

for master_loop in range(1,300): # range(x,y) Where x is starting number and y is ending number. Increments of 1. 
    create_tables()
    print ("Counting the number of users...")
    count_rows_users()
    print ("Counting the number of rows...")
    count_rows_tweets()
    delete_RTs()
    count_rows_tweets()
    setup_user_table()
    populate_users_table()
    for item in ST: #Loop through each ticker symbol.
        try_search = 0
        max_tries = 10
        while try_search < max_tries: 
            try:
                tweepy1_search(item)
                print (f"Success! on {item}, search try number: {try_search}")
                break
            except tweepy.error.TweepError as save_error: 
                time.sleep(10)
                print (save_error)
            try_search += 1
            print (f"Failed on {item}, retry number: {try_search}")            
            
        loop += 1
        midtime = int(time.perf_counter()) # Print an interim search time
        print ("Scraped tweets for:", item, "Searching for:", int((midtime-starttime)/60), "mins, or", int(((midtime-starttime)/60)/60), "hour/s.")
        count_rows_tweets()
        print ("Loop number:", loop, "Master loop:", master_loop, "\n")
    print ("Sleeping 30mins before starting another master loop...")
    time.sleep (1800)

#show the total scan time
finishtime = int(time.perf_counter()) #int the perf_counter float to an integer, easier to read
print("\nFinished scanning! Scantime:", int((finishtime-starttime)/60), "minutes. Or", int(((finishtime-starttime)/60)/60), "hour/s.")

db.commit()
db.close()