import json
import time
import os
import requests
import pickle
import logging
from databases import getsqlite, insert_into_price_table, get_mongo_conn, insert_into_price_coll
import datetime

def get_tickers(infile = 'picklejar/tickers.pickle'):
	with open(infile, 'rb') as cucumber:
		return pickle.load(cucumber)

def get_stock_prices(coll, ticker, sess):
	url = "http://finance.google.com/finance/info?client=ig&q=" + ticker
	response = sess.get(url)
	data = json.loads(response.text[3:])
	insert_into_price_coll(coll, data)
	# insert_into_price_table(conn, data)
	logging.debug('Adding %d records to stock data', len(data))

def stock_prices_run_forever(coll, tickers):

	os.environ['TZ'] = 'US/Eastern' #set timezone to EST for US market
	time.tzset()
	
	while True:
		now = datetime.datetime.now()
		sess = requests.session()
		# if ((now.hour >= 9 and now.minute >=30) or (now.hour > 9)) and now.hour < 16: # only get data during stock exchange hours (no cushion)
		try:
			if True:
			### TEMPORARILY COMMENTED OUT !!!!!!!!!
			# if now.hour >= 9 and (now.hour < 16 or (now.hour <= 16 and now.minute <= 30)): # only get data during stock exchange hours with half hour window on either side
				logging.debug("Acquiring Stock Data")
				print("Acquiring Stock Data")
				start = time.time()
				for t in tickers:
					get_stock_prices(coll, t, sess)
				remaining = 5 - (time.time() - start)
				if remaining > 0:
					time.sleep(remaining)
			else:
				time.sleep(5)
		except Exception:
			pass

if __name__=="__main__":
	logging.basicConfig(filename='stock.log', level=logging.DEBUG)
	# sqlconn = getsqlite('stocks2.sqlite3')
	tickers = get_tickers()
	conn = get_mongo_conn()
	coll = conn['tweetstock'].prices

	stock_prices_run_forever(coll, tickers)
