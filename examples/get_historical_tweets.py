from tweet_serializer import S3TweetSerializer, NBATweetSorter
from date_partitions import api_date_format, generate_dates
from auth.credentials import TWITTER_API_CREDENTIALS, AWS_CREDENTIALS

from datetime import datetime, timedelta
import tweepy
import logging
import sys

def get_s3_connection(aws_credentials = AWS_CREDENTIALS):
	from boto.s3.connection import S3Connection
	return S3Connection(aws_credentials['access_key'], aws_credentials['access_key_secret'])	

def get_twitter_api(twitter_credentials = TWITTER_API_CREDENTIALS):
	auth = tweepy.AppAuthHandler(twitter_credentials['consumer_key'], twitter_credentials['consumer_secret'])
	return tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

def get_nba_tweets(end_date = datetime.today().strftime(api_date_format), daysback = 7):
	end_date = datetime.strptime(end_date, api_date_format)

	if end_date < datetime.today() - timedelta(days=7):
		raise Exception('End date is beyond Twitter history limit')

	conn = get_s3_connection()
	api = get_twitter_api()

	for date in generate_dates(end_date - timedelta(days=7), end_date):
		# query each day separately
		next_date = date + timedelta(days=1)
		date, next_date = date.strftime(api_date_format), next_date.strftime(api_date_format)
		logging.warning("Gathering Twitter data from %s to %s", date, next_date)

		# initialize serializers for each type of input (i.e. warriors, finals, or both)
		both_ser = S3TweetSerializer(conn, 'bothtags101001', 'both-tags_' + date, chunksize=250)
		finals_ser = S3TweetSerializer(conn, 'finalstags101001', 'finals-tags_' + date, chunksize=250)
		warriors_ser = S3TweetSerializer(conn, 'warriorstags101001', 'warriors-tags_' + date, chunksize=250)

		# initialize sorting class, accepts serializers as arguments
		ts = NBATweetSorter(both_ser,finals_ser,warriors_ser)

		try:
			# sort the output from the tweepy cursor
			ts.sort(tweepy.Cursor(api.search,q='#nbafinals2015 OR #warriors', lang='en', since=date, until=next_date).items())
			ts.close_all()
			ts.show_counts()

		except KeyboardInterrupt:
			# in event of keyboard interrupt, close files before exit
			ts.close_all()
			ts.show_counts()
			sys.exit(1)

		except Exception, e:
			# in event of unknown error, log error then close files
			logging.error(e)
			ts.close_all()
			ts.show_counts()

if __name__=="__main__":
	logging.basicConfig(filename='logs/tweetlog.log', format="%(asctime)s %(message)s", level=logging.WARNING)
	get_nba_tweets()
