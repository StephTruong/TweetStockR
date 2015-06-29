import requests, time, datetime, pickle

ENDPOINT = 'http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,GOOG&f=nsl1op'

ENDPOINT2 = 'http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,GOOG&f=nsl1ops1i'

ENDPOINT3 = 'http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,GOOG,AAPL,QQQX,^GSPC,KO,COKE,MSFT,FTR,SIRI,CSCO,MU,SGYP,FOXA,FB,AAL,AMAT,CAFD,PAAS,QCOM&f=nshgb0b4c3r1a0b2a5t7d2s7'


def timer(endpoint):
	now = time.time()
	r = requests.get(endpoint)
	print "Request finished in %s seconds" % str(time.time() - now)
	text = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S,') + r.text
	print text 
	print r.text

def get_updates(fname, endpoint, delay, runtime):
	start = datetime.datetime.now()
	runtime = datetime.timedelta(seconds=runtime)
	reqs = 0
	with open(fname, 'wb') as f:
		while datetime.datetime.now() - start < runtime:
			r = requests.get(endpoint)
			text = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S,') + r.text
			f.write(text)
			print reqs; reqs += 1
			time.sleep(delay)
		f.close()
	print 'Done'

def get_update_time(fname, endpoint, delay, runtime):
	runtime = datetime.timedelta(seconds=runtime)
	reqs = 0
	start = datetime.datetime.now()
	nows = [datetime.datetime.now()]
	r = requests.get(endpoint)
	text = r.text
	# with open(fname, 'wb') as f:
	while datetime.datetime.now() - start < runtime:
		r = requests.get(endpoint)
		if text != r.text:
			nows.append(datetime.datetime.now())
			text = r.text
			print 'changed'
		print reqs; reqs += 1
		time.sleep(delay)
		# f.close()
	return nows

if __name__=="__main__":
	# timer(ENDPOINT)
	# timer(ENDPOINT2)
	# timer(ENDPOINT3)

	times = get_update_time('price_updates.csv', ENDPOINT, 1, 3600)
	pickle.dump(times, 'price_update_times.pickle')
