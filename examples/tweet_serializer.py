import boto, cStringIO, re, json, logging

class S3TweetSerializer(object):
	"""
	Serializes Tweets, saves to Amazon S3 bucket

	params:
	s3_conn			authenticated connection to S3
	bucket_name		name of bucket to initialize or access
	base_keyname 	base name of files to store in bucket
	chunksize 		max number of tweets per file	
	"""
	def __init__(self, s3_conn, bucket_name, base_keyname, chunksize=250):
		self._conn = s3_conn
		self._base_keyname = base_keyname
		self.bucket_name = bucket_name
		self._get_or_create_bucket() # connect to bucket
		self._chunksize = chunksize
		self._files_written = 0
		self._active_file = None
		self._tweets_written = 0

	def _get_or_create_bucket(self):
		logging.warning('Connecting to bucket %s', self.bucket_name)
		self._bucket = self._conn.lookup(self.bucket_name) # method returns none if bucket does not exist
		if self._bucket is None:
			logging.warning('No bucket named %s. Creating.', self.bucket_name)
			self._bucket = self._conn.create_bucket(self.bucket_name)
		

	def _write_active_file_to_bucket(self):
		s3key = boto.s3.key.Key(self._bucket) # new object of class Key
		s3key.key = "%s_%.04d.json" % (self._base_keyname, self._files_written) # set key name
		s3key.set_contents_from_string(self._active_file.getvalue()) # output tweets to key
		logging.warning("Wrote %d tweets to %s", self._tweets_written, s3key.key)

	def openf(self):
		if self._active_file is not None:
			self.closef() # always close open file before opening new one
		self._files_written += 1
		self._active_file = cStringIO.StringIO() # init file-like object to store tweets before writing
		self._active_file.write('[\n')

	def closef(self):
		if self._active_file is not None:
			self._active_file.write('\n]\n')
			self._write_active_file_to_bucket()
			self._active_file.close()
			self._active_file = None
			self._tweets_written = 0

	def writef(self, tweet_json):
		# conditions for writing tweets in memory to file
		if self._tweets_written >= self._chunksize or self._active_file is None:
			self.openf()
		if self._tweets_written > 0:
			self._active_file.write(",\n")
		self._active_file.write(json.dumps(tweet_json).encode('utf8'))
		self._tweets_written += 1


class NBATweetSorter(object):
	"""
	Sorts results from tweepy.Cursor by hashtag, then writes to file using appropriate serializer

	params:
	both_serializer		
	finals_serializer
	warriors_serializer
	"""
	def __init__(self, both_serializer, finals_serializer, warriors_serializer):
		self._both_ser = both_serializer
		self._finals_ser = finals_serializer
		self._warriors_ser = warriors_serializer
		self._all_serializers = [self._both_ser, self._finals_ser, self._warriors_ser]
		self.counts=[0,0,0,0]

	def close_all(self):
		# save and exit active files on all serializers
		for ser in self._all_serializers:
			ser.closef()

	def sort(self, cursor):
		# method to sort Cursor output before serializing tweets
		finalstag = re.compile("(#nbafinals2015)")
		teamtag = re.compile("(#warriors)")
		for tweet in cursor:
			# get regular expression matches
			matches = (re.search(finalstag, tweet.text.lower()), re.search(teamtag, tweet.text.lower()))
			if matches[0] and matches[1]: # if both hashtags are present, save to both-tags serializer
				self._both_ser.writef(tweet._json)
				self.counts[0] += 1
			elif matches[0] and not matches[1]: # etc
				self._finals_ser.writef(tweet._json)
				self.counts[1] += 1
			elif not matches[0] and matches[1]: # etc
				self._warriors_ser.writef(tweet._json)
				self.counts[2] += 1
			else:
				self.counts[3] += 1 # count tweets that contain neither, but do not serialize

	def show_counts(self):
		print "Counts [Both: %d, #NBAFinals2015: %d, #Warriors: %d, NA: %d]" % tuple(self.counts)


if __name__=="__main__":
	pass
	## TESTS
	# from boto.s3.connection import S3Connection
	# from auth.credentials import AWS_CREDENTIALS
	# ex_json = ['{"example%d":"sometext","2ndline":"othertext"}' % i for i in range(10)]
	# conn = S3Connection(AWS_CREDENTIALS['access_key'], AWS_CREDENTIALS['access_key_secret'])
	# test = TweetSerializer(conn, 'turtles1231234123', 'base_keyname', chunksize=5)
	# for i in ex_json:
	# 	test.writef(i)
	# test.closef()