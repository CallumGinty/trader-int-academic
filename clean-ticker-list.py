#!/usr/bin/env python
# coding: utf-8
import pandas


stocklist = pandas.read_excel("./ISIN.xls")

stocklist.dropna(axis=0, how='any', inplace=True) #drop empty rows

stocklist = stocklist[stocklist['Security type'] == "ORDINARY FULLY PAID"] #drop rows that arent ordinary fully paid shares

#drop any duplicate ASX code rows
stocklist.drop_duplicates(subset='Company name', keep="first", ignore_index=True) 

#reset the index after removing so many rows
stocklist.reset_index(drop=True, inplace=True) 

#hashtag append
searchterm = [ ("#" + stocklist ) for stocklist in stocklist["ASX code"]]
stocklist['hashtags'] = searchterm

#cashtag append
cashtag = [ ("$" + stocklist ) for stocklist in stocklist["ASX code"]]
stocklist['cashtags'] = cashtag


stocklist.to_csv(r'ISIN-cleaned.csv') #export out to a csv file

print("Cleaning complete!")

