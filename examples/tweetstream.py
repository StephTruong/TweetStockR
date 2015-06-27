from tweet_serializer import StreamSerializer
from auth.credentials import TWITTER_API_CREDENTIALS, AWS_CREDENTIALS
import datetime, time
import tweepy
# from get_historical_tweets import get_s3_connection

def get_twitter_stream(twitter_credentials = TWITTER_API_CREDENTIALS):
	auth = tweepy.auth.OAuthHandler(twitter_credentials['consumer_key'], twitter_credentials['consumer_secret'])
	auth.set_access_token(twitter_credentials['access_token'], twitter_credentials['access_token_secret'])
	return auth

if __name__=="__main__":
	auth = get_twitter_stream()
	listen = StreamSerializer('tps.csv', 600)
	sample = tweepy.Stream(auth, listen)
	sample.sample(languages=['en'])
