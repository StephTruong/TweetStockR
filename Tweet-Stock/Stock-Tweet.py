__author__ = 'HowardW'

import sqlite3
import datetime, time
from datetime import timedelta,datetime as dt
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

def main():
    # Note you need to unpack the .sql database from Hank and then connect to it via the conn below
    conn = sqlite3.connect('database.db')
    c = conn.cursor()

    datef="%Y-%m-%d %H:%M:%S.%f"
    datef2="'%Y-%m-%d %H:%M:%S'"
    datef3="'%Y-%m-%d'"
    #Calculating how much time has passed since collection of the first row
    c.execute('SELECT datetime FROM sentiment ASC LIMIT 1')
    firstdate = c.fetchone()
    lag=dt.now()-dt.strptime(firstdate[0],datef) #this is for simulation purpose, we want to simulate real time processing with a lag
    # In production we could put a time lag of 15 minutes 
    #lag=timedelta(minutes=15)

    #c.execute('SELECT * FROM sentiment WHERE company=?', (t,))
    #for items in c.execute('SELECT * FROM sentiment where company='+str(t)+' LIMIT 10'):
        #print items

    while True: # doing a infinite loop
      
      sleeptime = 60 - dt.now().second
      #print "Sleeping "+str(sleeptime)+"sec until start of a minute"
      #time.sleep(sleeptime)


      starttime=dt.now()-lag+timedelta(hours=4)
      endtime= starttime+timedelta(minutes=1)
      # d = "\'2015-08-12 13:16:00\'"
      # e = "\'2015-08-12 16:17:00\'"

      # Search for distinct stocks that were Tweeted
      for item1 in c.execute('SELECT DISTINCT company FROM sentiment where datetime BETWEEN '+dt.strftime(starttime,datef2)+' AND '+dt.strftime(endtime,datef2)):
          countSentiment = 0
          countStock = 0
          ticker = item1[0]
          sumNewSentiment = 0
          sumOldSentiment = 0
          countNewSentiment = 0
          countOldSentiment = 0
          sumNewStock = 0
          sumOldStock = 0
          countNewStock = 0
          countOldStock = 0

          # Check to see if tweet record is 200 or greater
          c.execute('SELECT count(*) FROM sentiment where company= \''+str(ticker)+'\'AND datetime<"'+str(starttime)+ '"' )
          tweet_total = c.fetchone()
          if tweet_total[0] < 200:
              continue
          print ('Company: ' + str(ticker) + ', Tweet record: ' + str(tweet_total[0]))


          # Check to see if Stock prices are available
          c.execute('SELECT count(*) FROM prices where name = \''+str(ticker.upper())+'\'AND datetime<"'+str(starttime)+ '"')
          stock = c.fetchone()
          if stock[0] < 720:
              print ('Company: ' + str(ticker) + ', Stock record not found.')
              continue

          # Searches for the last 1000 sentiment readings
          for item2 in c.execute('SELECT * FROM sentiment where company= \''+str(ticker)+
                                         '\' AND datetime<"'+str(starttime)+ '" ORDER BY date(datetime) DESC LIMIT 1000'):
              countSentiment += 1
              if countSentiment < 101:
                  sumNewSentiment += item2[1]
                  countNewSentiment += 1
              else:
                  sumOldSentiment += item2[1]
                  countOldSentiment += 1

          averageNewSentiment = sumNewSentiment / countNewSentiment
          averageOldSentiment = sumOldSentiment / countOldSentiment
          
          # Finds the average of the first 200 and last 800 sentiment readings
          print (ticker + ' New: ' + str(averageNewSentiment) + ' Old: ' + str(averageOldSentiment))
          #print datetime.datetime.now()

          # Searches for the last 1000 prices for the stock
          for item3 in c.execute('SELECT * FROM prices where name = \''+str(ticker.upper())+
                                           '\' AND datetime<"'+str(starttime)+ '" ORDER BY date(recorded) DESC LIMIT 720'):
              countStock += 1
              if countStock < 61:
                  sumNewStock += item3[4]
                  countNewStock += 1
              else:
                  sumOldStock += item3[4]
                  countOldStock += 1

          averageNewStock = sumNewStock / countNewStock
          averageOldStock = sumOldStock / countOldStock

          # Finds the average price of the first 5 minutes and last 55 minutes
          try:
              print str(ticker) + ' New: ' + str(averageNewStock) + ' Old: ' + str(averageOldStock)
              #print datetime.datetime.now()

              print
              if averageNewSentiment > averageOldSentiment and averageNewStock > averageOldStock:
                  print "\tBuy: " + str(ticker)
              elif averageNewSentiment < averageOldSentiment and averageNewStock < averageOldStock:
                  print "\tSell: " + str(ticker)
              else:
                  print "\tHold: " + str(ticker)
              print


          except ZeroDivisionError:
              print 'Stock not found '+str(ticker) 
              print datetime.datetime.now() -lag


      if dt.now()-lag >endtime:
        print "!!!!it took more than one min! your code is too slow!!!!"
      

if __name__ == '__main__':
    main()
