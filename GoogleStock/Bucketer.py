import boto, json


class Bucketer(object):
	# simple s3 serializer for json objects
	# conn argument is an authorized connection to S3
	def __init__(self, conn, bucketname, basekeyname, chunksize=250):
		print "init"
		self._get_or_create_bucket(conn, bucketname)
		self.chunksize = chunksize
		self.tickers = []
		self.basekeyname = basekeyname
		self._files_written = 0
 
	def _get_or_create_bucket(self, conn, bucketname):
		self.bucket = conn.lookup(bucketname)
		if self.bucket is None:
				self.bucket = conn.create_bucket(bucketname)

	def add_ticker(self, ticker):
		self.tickers.append(ticker)
		if len(self.tickers) >= self.chunksize:
			self.save_tickers()
 

	def save_tickers(self):
		print "new file"
		key = boto.s3.key.Key(self.bucket)
		key.key = "%s_%.04d.json" % (self.basekeyname, self._files_written)
		key.set_contents_from_string(json.dumps(self.tickers, ensure_ascii=False)+"\n")
		self._files_written += 1
		self.tickers = []
		
