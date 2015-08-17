__author__ = 'HowardW'

# import sqlite3
import datetime, time
from datetime import timedelta,datetime as dt
import sys
from databases import get_mongo_conn
import pymongo as pmg
import os
import multiprocessing

reload(sys)
sys.setdefaultencoding('UTF8')


def get_predictions(ticker):
	conn = get_mongo_conn()
	prices = conn['tweetstock'].prices
	sentiment = conn['tweetstock'].sentiment
	predictions = conn['tweetstock'].predictions

	try:
		countSentiment = 0
		countStock = 0
		sumNewSentiment = 0
		sumOldSentiment = 0
		countNewSentiment = 0
		countOldSentiment = 0
		sumNewStock = 0
		sumOldStock = 0
		countNewStock = 0
		countOldStock = 0

		# Check to see if tweet record is 200 or greater
		recent_sentiment = sentiment.find({'company': ticker}).limit(1000).sort('datetime', pmg.DESCENDING)
		if recent_sentiment.count() < 101:
			return
		print ('Company: ' + str(ticker) + ', Tweet record: ' + str(recent_sentiment.count()))

		# Check to see if Stock prices are available
		recent_prices = prices.find({'name': ticker.upper()}).limit(720).sort('recorded', pmg.DESCENDING)
		if recent_prices.count() < 61:
			print ('Company: ' + str(ticker) + ', Stock record not found.')
			return

		# Searches for the last 1000 sentiment readings
		for sent in recent_sentiment:
			countSentiment += 1
			if countSentiment < 101:
				sumNewSentiment += float(sent['score'])
				countNewSentiment += 1
			else:
				sumOldSentiment += float(sent['score'])
				countOldSentiment += 1

		averageNewSentiment = float(sumNewSentiment) / countNewSentiment
		averageOldSentiment = float(sumOldSentiment) / countOldSentiment
		
		# Finds the average of the first 200 and last 800 sentiment readings
		print ('SENTIMENT: ' + ticker + ' New: ' + str(averageNewSentiment) + ' Old: ' + str(averageOldSentiment))
		#print datetime.datetime.now()

		# Searches for the last 720 prices for the stock
		for price in recent_prices:
			countStock += 1
			if countStock < 61:
				sumNewStock += float(price['price'])
				countNewStock += 1
			else:
				sumOldStock += float(price['price'])
				countOldStock += 1

		averageNewStock = float(sumNewStock) / countNewStock
		averageOldStock = float(sumOldStock) / countOldStock

		# Finds the average price of the first 5 minutes and last 55 minute
		print 'STOCK PRICE: ' + str(ticker) + ' New: ' + str(averageNewStock) + ' Old: ' + str(averageOldStock)
		#print datetime.datetime.now()

		if averageNewSentiment > averageOldSentiment + .0000001 and averageNewStock > averageOldStock + .0000001:
			prediction = 'BUY'
		elif averageNewSentiment < averageOldSentiment - .0000001 and averageNewStock < averageOldStock - .0000001:
			prediction = 'SELL'
		else:
			prediction = 'HOLD'

		recent_prediction = predictions.find({'stock': ticker}).sort('datetime', pmg.DESCENDING).limit(1)
		if recent_prediction.count() > 0:
			last_prediction = list(recent_prediction)[0]
			if last_prediction['prediction'] == prediction:
				streak_n = last_prediction['streak'] + 1
			else:
				streak_n = 0
		else:
			streak_n=0

		predictions.insert_one({'stock': ticker, 'datetime': dt.now(), 'prediction': prediction, 'streak': streak_n})
		out = ','.join([str(i) for i in [dt.now().strftime("%Y%m%d%H%M%S"), ticker, prediction, streak_n]]) + '\n'
		print out
		# prediction_file.write( out )

	except ZeroDivisionError:
		print 'Stock not found '+str(ticker)

	conn.close()
	return


def main(outfile):

	os.environ['TZ'] = 'US/Eastern' #set timezone to EST for US market
	time.tzset()
	
	conn = get_mongo_conn()
	# prices = conn['tweetstock'].prices
	sentiment = conn['tweetstock'].sentiment
	# predictions = conn['tweetstock'].predictions

	while True: # doing a infinite loop

		pool = multiprocessing.Pool(4)

		start = time.time()

		starttime=dt.now()-timedelta(minutes=5)
		endtime=dt.now()-timedelta(minutes=0)

		# Search for distinct stocks that were recently tweeted
		recent_tickers = list(sentiment.find({'datetime':{'$lt':endtime, '$gt':starttime}, 'company':{'$ne':None}}).distinct('company'))

		try:
			print 'Making predictions for %d companies' % len(recent_tickers)
			pool.map_async(func=get_predictions, iterable=recent_tickers).get(6000)
		except KeyboardInterrupt:
			pool.terminate()
			# raise KeyboardInterrupt('Stopped with KeyboardInterrupt')

		print "predictions took", time.time() - start, 'seconds'
	  
if __name__ == '__main__':
	main('predictions.csv')
