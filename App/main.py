DOCSTRING = """Main TweetStock Execution Code"""

import sys
import logging
import subprocess
from multiprocessing import Process
from tweet_stream import main
# from stock import stock_prices_run_forever, get_tickers
# from databases import getsqlite

def worker(args):
	subprocess.call(list(args))


if __name__=="__main__":
	# logging.basicConfig(filename='main.log', level=logging.DEBUG)
	# tickers = get_tickers()
	# sqlconn = getsqlite()

	arguments = [('python','tweet_stream.py'), ('python','stock.py'), ('python','buy_or_sell.py')]

	processes = [Process(target=worker, args=(args,)) for args in arguments]
	for p in processes:
		p.daemon=True
	for p in processes:
		p.start()
	for p in processes:
		p.join()

