#extract_predictions.py

from databases import get_mongo_conn
import json
import datetime as dt
import os
import time

if __name__=="__main__":

	os.environ['TZ'] = 'US/Eastern' #set timezone to EST for US market
	time.tzset()

	conn = get_mongo_conn()

	prices =conn['tweetstock'].prices
	sentiment =conn['tweetstock'].sentiment
	predictions =conn['tweetstock'].predictions

	print 'prices'
	with open('prices.json','wb') as pr:
		outlist=[]
		for item in prices.find({'datetime':{'$gt':dt.datetime.now() - dt.timedelta(days=1)}},{'_id':False, 'recorded':False}):
			item2 = item.copy()
			item2['datetime'] = item2['datetime'].strftime('%Y%m%d%H%M%S')
			outlist.append(item2)
		json.dump(outlist, pr)
	print 'prices finished'
	print 'sentiment'
	with open('sentiment.json','wb') as pr:
		outlist=[]
		for item in sentiment.find({'datetime':{'$gt':dt.datetime.now() - dt.timedelta(days=1)}},{'_id':False}):
			item2 = item.copy()
			item2['datetime'] = item2['datetime'].strftime('%Y%m%d%H%M%S')
			outlist.append(item2)
		json.dump(outlist, pr)
	print 'sentiment finished'
	print 'predictions'
	with open('predictions.json','wb') as pr:
		outlist=[]
		for item in predictions.find({'datetime':{'$gt':dt.datetime.now() - dt.timedelta(days=1)}},{'_id':False}):
			item2 = item.copy()
			item2['datetime'] = item2['datetime'].strftime('%Y%m%d%H%M%S')
			outlist.append(item2)
		json.dump(outlist, pr)
	print 'predictions finished'
