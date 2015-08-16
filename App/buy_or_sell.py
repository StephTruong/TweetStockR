__author__ = 'HowardW'

# import sqlite3
import datetime, time
from datetime import timedelta,datetime as dt
import sys
from databases import get_mongo_conn
import pymongo as pmg
import os

reload(sys)
sys.setdefaultencoding('UTF8')

def main(outfile):

	os.environ['TZ'] = 'US/Eastern' #set timezone to EST for US market
	time.tzset()
	
	conn = get_mongo_conn()
	prices = conn['tweetstock'].prices
	sentiment = conn['tweetstock'].sentiment
	predictions = conn['tweetstock'].predictions

	with open(outfile, 'ab') as prediction_file:

	  while True: # doing a infinite loop
		
		sleeptime = 60 - dt.now().second
		print "Sleeping "+str(sleeptime)+"sec until start of a minute"
		time.sleep(sleeptime)
		print 'Starting Predictions'

		starttime=dt.now()-timedelta(hours=4)
		endtime=dt.now()-timedelta(minutes=0)

		# Search for distinct stocks that were recently tweeted
		for item in sentiment.find({'datetime':{'$lt':endtime, '$gt':starttime}}):
			try:
				print item
				countSentiment = 0
				countStock = 0
				ticker = item['name']
				sumNewSentiment = 0
				sumOldSentiment = 0
				countNewSentiment = 0
				countOldSentiment = 0
				sumNewStock = 0
				sumOldStock = 0
				countNewStock = 0
				countOldStock = 0

				# Check to see if tweet record is 200 or greater
				recent_sentiment = sentiment.find({'company': ticker, 'datetime': {'$lt':endtime, '$gt':starttime}}).limit(1000).sort('datetime', pmg.DESCENDING)
				if recent_sentiment.count() < 100:
					continue
				print ('Company: ' + str(ticker) + ', Tweet record: ' + str(recent_sentiment.count()))


				# Check to see if Stock prices are available
				recent_prices = prices.find({'name': ticker.upper(), 'datetime': {'$lt':endtime, '$gt':starttime}}).limit(720)
				if recent_prices < 720:
					print ('Company: ' + str(ticker) + ', Stock record not found.')
					continue

				# Searches for the last 1000 sentiment readings

				for sent in recent_sentiment:
					countSentiment += 1
					if countSentiment < 101:
						sumNewSentiment += sent['score']
						countNewSentiment += 1
					else:
						sumOldSentiment += sent['score']
						countOldSentiment += 1

				averageNewSentiment = sumNewSentiment / countNewSentiment
				averageOldSentiment = sumOldSentiment / countOldSentiment
				
				# Finds the average of the first 200 and last 800 sentiment readings
				print (ticker + ' New: ' + str(averageNewSentiment) + ' Old: ' + str(averageOldSentiment))
				#print datetime.datetime.now()

				# Searches for the last 720 prices for the stock
				for price in recent_prices:
					countStock += 1
					if countStock < 61:
						sumNewStock += price['price']
						countNewStock += 1
					else:
						sumOldStock += price['price']
						countOldStock += 1

				averageNewStock = sumNewStock / countNewStock
				averageOldStock = sumOldStock / countOldStock

				# Finds the average price of the first 5 minutes and last 55 minute
				print str(ticker) + ' New: ' + str(averageNewStock) + ' Old: ' + str(averageOldStock)
				#print datetime.datetime.now()

				print
				if averageNewSentiment > averageOldSentiment and averageNewStock > averageOldStock:
					prediction = 'BUY'
				elif averageNewSentiment < averageOldSentiment and averageNewStock < averageOldStock:
					prediction = 'SELL'
				else:
					prediction = 'HOLD'

				recent_prediction = predictions.find({'stock': ticker}).sort('datetime', pmg.DESCENDING).limit(1)
				if recent_prediction.count() > 0:
					last_prediction = list(recent_prediction)[0]
					if last_prediction['prediction'] == prediction:
						streak_n = recent_prediction['streak'] + 1
					else:
						streak_n = 0
				else:
					streak_n=0

				predictions.insert_one({'stock': ticker, 'datetime': dt.now(), 'prediction': prediction, 'streak': streak_n})
				out = ','.join([dt.now().strftime("%Y%m%d%H%M%S"), ticker, prediction, streak_n]) + '\n'
				print out
				prediction_file.write( out )

			except ZeroDivisionError:
				print 'Stock not found '+str(ticker) 
	  
if __name__ == '__main__':
	main('predictions.csv')
