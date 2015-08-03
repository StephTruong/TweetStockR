import json
import datetime
import sqlite3 as sql
import pymongo as pm

def getsqlite(name):
	return sql.connect(name)

def sqlite_setup(conn):
	cursor = conn.cursor()
	# cursor.execute("CREATE TABLE prices (id integer, name text, datetime timestamp, price real, cur_price real, fix_price real)")
	cursor.execute("CREATE TABLE sentiment (id integer, score real, datetime timestamp, company text, retweet integer)")

def insert_into_price_table(conn, ticker):
	cursor = conn.cursor()
	data = []
	dt_fmt = "%Y-%m-%dT%H:%M:%SZ"
	for tick in ticker:
		try:
			data.append( ( tick['id'], tick['t'], datetime.datetime.strptime(tick['lt_dts'], dt_fmt) , tick['l'], tick['l_cur'], tick['l_fix'] ) )
		except Exception:
			pass
	# dump prices to SQLite
	cursor.executemany("INSERT INTO prices VALUES (?,?,?,?,?,?)", data)
	conn.commit()

def insert_into_sentiment_table(conn, cursor, tweet):
	# dump sentiment to SQLite
	cursor.execute("INSERT INTO sentiment VALUES (?,?,?,?)", data)
	conn.commit()

def get_mongo_conn():
	return pm.MongoClient()

def dump_collection_to_s3(coll, bucket, keyname):
	data = list(coll.find())
	key = Key(bucket)
	key.key = keyname
	key.set_contents_from_string(json.dumps(data, ensure_ascii=False))

def upsert_tweet(coll, tweet):
	status = coll.update( {'id': tweet['id']}, tweet, upsert=True )
	return status['updatedExisting']

if __name__=="__main__":
	pass
	## SET UP
	# conn = getsqlite('db/stocks.sqlite3')
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

