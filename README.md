# Trader-int Academic
Twitter and Botometer extractor.

## Description:
This software extracts Tweets for a list of ASX cashtags and assigns bot scores to the users. Results are saved in two tables in a MySQL database. 

This software was developed as part of a masters thesis.



## Requirements:
Python3.9 (or above),
MySQL server,
Twitter API key,
Botometer API key


### Step 1: 
Input your server settings in database_config.py
Input your API keys in api_keys.py

### Step 2:
To obtain the ticker list, run "Download-ticker-list.sh"
Or download the .xls file directly: https://www2.asx.com.au/content/dam/asx/issuers/ISIN.xls

### Step 3:
Extract cashtags and clean up the ticker list with:
clean-ticker-list.py

### Step 4:
Begin Twitter scraper with:
run-search.py

### Step 5:
Begin Botometer calls with:
get-bot-scores.py
