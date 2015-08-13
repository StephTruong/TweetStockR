__author__ = 'HowardW'

import sqlite3
import datetime
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

def main():
    # Note you need to unpack the .sql database from Hank and then connect to it via the conn below
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    cc = conn.cursor()
    ccc = conn.cursor()
    d = "\'2015-08-08 11:16:50\'"
    e = "\'2015-08-08 11:17:50\'"
    #c.execute('SELECT * FROM sentiment WHERE company=?', (t,))
    #for items in c.execute('SELECT * FROM sentiment where company='+str(t)+' LIMIT 10'):
        #print items

    # Search for distinct stocks that were Tweeted
    for item1 in cc.execute('SELECT DISTINCT company FROM sentiment where datetime BETWEEN '+str(d)+' AND '+str(e)):
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

        # Check to see if tweet record is 1000 or greater
        c.execute('SELECT count(*) FROM sentiment where company= \''+str(item1[0])+'\'')
        company = c.fetchone()
        print ('Company: ' + str(item1[0]) + ', Tweet record: ' + str(company[0]))
        if company[0] < 1000:
            print
            continue

        # Check to see if Stock prices are available
        ccc.execute('SELECT count(*) FROM prices where name = \''+str(item1[0].upper())+'\'')
        stock = ccc.fetchone()
        if stock[0] < 720:
            print ('Company: ' + str(item1[0]) + ', Stock record not found.')
            print
            continue

        # Searches for the last 1000 sentiment readings
        for item2 in c.execute('SELECT * FROM sentiment where company= \''+str(item1[0])+
                                       '\' ORDER BY date(datetime) DESC LIMIT 1000'):
            countSentiment += 1
            if countSentiment < 201:
                sumNewSentiment += item2[1]
                countNewSentiment += 1
            else:
                sumOldSentiment += item2[1]
                countOldSentiment += 1

        averageNewSentiment = sumNewSentiment / countNewSentiment
        averageOldSentiment = sumOldSentiment / countOldSentiment
        
        # Finds the average of the first 200 and last 800 sentiment readings
        print (ticker + ' New: ' + str(averageNewSentiment) + ' Old: ' + str(averageOldSentiment))
        print datetime.datetime.now()

        # Searches for the last 1000 prices for the stock
        for item3 in ccc.execute('SELECT * FROM prices where name = \''+str(item1[0].upper())+
                                         '\' ORDER BY date(recorded) DESC LIMIT 720'):
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
            print ticker + ' New: ' + str(averageNewStock) + ' Old: ' + str(averageOldStock)
            print datetime.datetime.now()

            print
            if averageNewSentiment > averageOldSentiment and averageNewStock > averageOldStock:
                print "\tBuy: " + str(item1[0])
            elif averageNewSentiment < averageOldSentiment and averageNewStock < averageOldStock:
                print "\tSell: " + str(item1[0])
            else:
                print "\tHold: " + str(item1[0])
            print

        except ZeroDivisionError:
            print 'Stock not found'
            print datetime.datetime.now()



if __name__ == '__main__':
    main()