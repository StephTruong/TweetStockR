import requests, csv, time

ENDPOINT = 'http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,GOOG&f=nsl1op'

ENDPOINT2 = 'http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,GOOG&f=nsl1ops1i'

ENDPOINT3 = 'http://download.finance.yahoo.com/d/quotes.csv?s=%40%5EDJI,GOOG,AAPL,QQQX,^GSPC,KO,COKE,MSFT,FTR,SIRI,CSCO,MU,SGYP,FOXA,FB,AAL,AMAT,CAFD,PAAS,QCOM&f=nshgb0b4c3r1a0b2a5t7d2s7'


def timer(endpoint):
	now = time.time()
	r = requests.get(endpoint)
	print "Request finished in %s seconds" % str(time.time() - now)
	print r.text

if __name__=="__main__":
	timer(ENDPOINT)
	timer(ENDPOINT2)
	timer(ENDPOINT3)

