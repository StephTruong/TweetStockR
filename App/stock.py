import json
import time
import requests
import pickle
from databases import getsqlite, insert_into_price_table

def get_tickers(infile = 'tickers.pickle'):
	with open(infile, 'rb') as cucumber:
		return pickle.load(cucumber)

def get_stock_prices(conn, ticker):
	url = "http://finance.google.com/finance/info?client=ig&q=" + ticker
	response = requests.get(url)
	data = json.loads(response.text[3:])
	print len(data)
	insert_into_price_table(conn, data)

if __name__=="__main__":
	
	sqlconn = getsqlite()
	tickers = get_tickers()

	while True:
		print 'go'
		start = time.time()
		for t in tickers:
			get_stock_prices(sqlconn, t)
		remaining = 5 - (time.time() - start)
		print "sleeping", remaining
		time.sleep(remaining)

