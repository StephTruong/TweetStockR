import json
import datetime
import sqlite3 as sql
import pymongo as pm
import logging
from datetime import datetime
from aws import get_s3_connection, write_string_to_key

# DEPRECATED
def getsqlite(name='db/stocks.sqlite3'):
	return sql.connect(name)

# DEPRECATED
def sqlite_setup(conn):
	cursor = conn.cursor()
	cursor.execute("CREATE TABLE prices (id integer, name text, recorded timestamp, datetime timestamp, price real)")
	cursor.execute("CREATE TABLE sentiment (id integer, score real, datetime timestamp, company text, retweet integer)")

def insert_into_price_coll(coll, ticker):
	# inserts stock ticker data into mongo db collection
	data = []
	dt_fmt = "%Y-%m-%dT%H:%M:%SZ"
	for tick in ticker:
		try:
			data.append(
				{'id':tick['id'],
				'name': tick['t'],
				'recorded': datetime.now(),
				'datetime': datetime.strptime(tick['lt_dts'], dt_fmt),
				'price': tick['l']}
				)
		except Exception:
			pass
	# dump prices to SQLite
	coll.insert_many(data)

def insert_into_sentiment_coll(coll, mltweet):
	tweet, score, comps = mltweet
	if len(comps[1]) > 0: # if multiple companies were identified
		for comp in comps[1]:
			data = {
			'_id':tweet['id'],
			'score': score[0],
			'datetime': datetime.now(),
			'company': comp,
			'retweet': comps[0]
			}
			coll.insert_one(data)
	else: 
		data = {
			'_id':tweet['id'],
			 'score': score[0],
			 'datetime': datetime.now(),
			 'company': None,
			 'retweet': comps[0]
			 }
		coll.insert_one(data)

# DEPRECATED
def insert_into_price_table(conn, ticker):
	cursor = conn.cursor()
	data = []
	dt_fmt = "%Y-%m-%dT%H:%M:%SZ"
	for tick in ticker:
		try:
			data.append( ( tick['id'], tick['t'], datetime.now(), datetime.strptime(tick['lt_dts'], dt_fmt) , tick['l'] ) )
		except Exception:
			pass
	# dump prices to SQLite
	cursor.executemany("INSERT INTO prices VALUES (?,?,?,?,?)", data)
	conn.commit()

# DEPRECATED
def insert_into_sentiment_table(conn, cursor, mltweet):
	tweet, score, comps = mltweet
	if len(comps[1]) > 0:
		for comp in comps[1]:
			data = ( tweet['id'], score[0], datetime.now(), comp, comps[0] ) 
			# dump sentiment to SQLite
			cursor.execute("INSERT INTO sentiment VALUES (?,?,?,?,?)", data)
	else:
		data = ( tweet['id'], score[0], datetime.now(), None , comps[0] ) 
		cursor.execute("INSERT INTO sentiment VALUES (?,?,?,?,?)", data)
	conn.commit()

def get_mongo_conn():
	return pm.MongoClient()

def dump_collection_to_s3(coll, bucket, basename):
	logging.info('dumping collection to s3')
	name = basename + "_" + datetime.now().strftime('%Y%m%d%H%M%S') + '.json'
	data = list(coll.find())
	logging.info('Dropping mongo')
	coll.drop()
	write_string_to_key(bucket, name, json.dumps(data, ensure_ascii=False))

dict_extract = lambda x, y: dict(zip(x, map(y.get, x)))
# dict_extract snippet from http://code.activestate.com/recipes/115417-subset-of-a-dictionary/


def upsert_tweet(coll, tweet, bucket, dump_after=2500):
	status = coll.update( {'id': tweet['id']}, dict_extract( ['id','text','timestamp_ms','created_at'], tweet), upsert=True )
	if coll.count() >= dump_after:
		dump_collection_to_s3(coll, bucket, 'tweetdump')
	return status['updatedExisting']

if __name__=="__main__":
	logging.basicConfig(filename='db.log', level=logging.DEBUG)
	pass
	# SET UP
	# conn = getsqlite('db/stocks2.sqlite3')
	# cur = conn.cursor()
	# cur.execute("DROP TABLE if exists sentiment")
	# cur.execute("DROP TABLE if exists prices")
	# sqlite_setup(conn)
	# conn = getsqlite('db/tweets.sqlite3')
	# cur = conn.cursor()
	# cur.execute("DROP TABLE if exists prices")
	# cur.execute("DROP TABLE if exists sentiment")
	# sqlite_setup(conn)
	# conn.close()

	# TEST UPLOAD
	# with open('test.json') as js:
	# 	prices = json.load(js)

	# conn = getsqlite('db/stocks.sqlite3')
	# # insert_into_price_table(conn, prices)
	# cur = conn.cursor()
	# for row in cur.execute('SELECT * FROM prices'):
	# 	print row
	# cur.execute("DELETE FROM prices")
	# conn.commit()
	# for row in cur.execute('SELECT * FROM prices'):
	# 	print row

