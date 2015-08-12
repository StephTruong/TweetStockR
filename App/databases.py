import json
import datetime
import sqlite3 as sql
import pymongo as pm
import logging
from datetime import datetime
from aws import get_s3_connection, write_string_to_key

def getsqlite(name='db/stocks.sqlite3'):
	return sql.connect(name)

def sqlite_setup(conn):
	cursor = conn.cursor()
	cursor.execute("CREATE TABLE prices (id integer, name text, recorded timestamp, datetime timestamp, price real)")
	cursor.execute("CREATE TABLE sentiment (id integer, score real, datetime timestamp, company text, retweet integer)")

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

def upsert_tweet(coll, tweet, bucket):
	status = coll.update( {'_id': tweet['id']}, tweet, upsert=True )
	if coll.count() > 1000:
		dump_collection_to_s3(coll, bucket, 'tweetdump')
	return status['updatedExisting']

if __name__=="__main__":
	logging.basicConfig(filename='db.log', level=logging.DEBUG)
	pass
	# SET UP
	conn = getsqlite('db/stocks.sqlite3')
	cur = conn.cursor()
	cur.execute("DROP TABLE prices")
	cur.execute("DROP TABLE sentiment")
	sqlite_setup(conn)
	conn.close()

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

