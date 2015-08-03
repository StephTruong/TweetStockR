import json
import time
import requests
import pickle
import logging
from databases import getsqlite, insert_into_price_table

def get_tickers(infile = 'tickers.pickle'):
	with open(infile, 'rb') as cucumber:
		return pickle.load(cucumber)

def get_stock_prices(conn, ticker):
	url = "http://finance.google.com/finance/info?client=ig&q=" + ticker
	response = requests.get(url)
	data = json.loads(response.text[3:])
	insert_into_price_table(conn, data)
	logging.debug('Adding %d records to stock data', len(data))

if __name__=="__main__":
	
	sqlconn = getsqlite()
	tickers = get_tickers()

	while True:
		logging.debug("Acquiring Stock Data")
		start = time.time()
		for t in tickers:
			get_stock_prices(sqlconn, t)
		remaining = 5 - (time.time() - start)
		time.sleep(remaining)
