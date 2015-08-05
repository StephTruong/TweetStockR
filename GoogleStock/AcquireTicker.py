import urllib2
import json
import time
from Bucketer import Bucketer
import boto
from boto.s3.connection import S3Connection
import os, time
import csv


class acquireTicker():
    
    def __init__(self,tickerList):
        self.prefix = "http://finance.google.com/finance/info?client=ig&q="
        self.tickerList=tickerList

    def get(self):
        url = self.prefix+self.tickerList
        u = urllib2.urlopen(url)
        content = u.read()
        obj = json.loads(content[3:])
        return obj


def getSymbol(filename):
    filename
    mFile = open(filename,'rU') 
    reader =csv.reader(mFile)
    symbolString=""
    first=True
    for i in reader:
        if not first:
            symbolString=symbolString+','
        symbolString=symbolString+i[2]+':'+i[0]
        first=False
    return symbolString

if __name__ == "__main__":
    

    
    sfiles=['tickerlistHW1.csv','tickerlistHW2.csv','tickerlistHW3.csv']
    symbolList=[]
    for s in sfiles:
        symbolList.append(getSymbol(s))
        

    r1 = acquireTicker(symbolList[0])
    r2 = acquireTicker(symbolList[1])
    r3 = acquireTicker(symbolList[2])
 

    AWS_ACCESS_KEY_ID = 'AKIAJUVUHL3HOBJERN4A'
    AWS_SECRET_ACCESS_KEY='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'

    conn= S3Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
    date = time.strftime('%Y-%m-%d')
    myBucket= Bucketer(conn,'st_tweetstock_finance',date, chunksize=10) 
    
    while 1:
        quote = r1.get()
        for q in quote:
            myBucket.add_ticker(q)
        quote = r2.get()
        for q in quote:
            myBucket.add_ticker(q)
        quote = r3.get()
        for q in quote:
            myBucket.add_ticker(q)
        print "added"
        time.sleep(5)