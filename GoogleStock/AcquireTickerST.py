import urllib2
import json
import time
from Bucketer import Bucketer
import boto
from boto.s3.connection import S3Connection
import os, time



class acquireTicker():
    
    def __init__(self,symbol,exchange):
        self.prefix = "http://finance.google.com/finance/info?client=ig&q="
        self.symbol=symbol
        self.exchange=exchange
    def get(self):
        url = self.prefix+"%s:%s"%(self.exchange,self.symbol)
        u = urllib2.urlopen(url)
        content = u.read()
        obj = json.loads(content[3:])
        return obj[0]





if __name__ == "__main__":
    
    os.environ['TZ'] = 'US/Eastern' #set timezone to EST for US market
    time.tzset()
    
    exchange="NASDAQ"
    symbol="MSFT"
    r = acquireTicker(symbol,exchange)

    AWS_ACCESS_KEY_ID = ''
    AWS_SECRET_ACCESS_KEY=''
    
    conn= S3Connection(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
    date = time.strftime('%Y-%m-%d')
    myBucket= Bucketer(conn,'st.tweetstock.finance',exchange+'_'+symbol+'_'+date, chunksize=250) 
    
    while 1:
        quote = r.get()
        myBucket.add_ticker(quote)
        time.sleep(5)