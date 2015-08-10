import pickle
import re
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.externals import joblib
import time
from databases import insert_into_sentiment_table

def get_stock_symbols(fname = 'picklejar/tickers.pickle'):
	with open(fname, 'rb') as f:
		tickers = pickle.load(f)
	out = []
	for tickerlist in tickers:
		for ticker in tickerlist.split(','):
			out.append(ticker.split(':')[1].lower())
	return out

def get_countvectorizer(fname = 'picklejar/count_vectorizer.pickle'):
	return joblib.load(fname)

def get_naivebayes(fname='picklejar/naive_bayes.pickle'):
	return joblib.load(fname)

def tweet_preprocessor(s):
    s = s.lower()
    s = re.sub(r'@\w+',' tweetat ',s)
    s = re.sub(r'\d+',' numerictoken ', s) # replace all numbers with " numerictoken " token
    s = re.sub(r'_+', ' ', s)
    s = re.sub(r'(http\S+)', ' URL ', s)
    return s

def check_for_companies_and_retweets(transformed_tweet, vocab, symbols):
	comps = []
	for symbol in symbols:
		try:
			if vocab[symbol] in transformed_tweet.indices:
				comps.append(symbol)
		except KeyError:
			# having problems with m, bf/b, etc etc
			pass
	if vocab['rt'] in transformed_tweet.indices:
		rt = 1
	else:
		rt = 0
	return (rt, comps)

def ml_process_tweet(tweet, cv, nb, symbols ):
	transformed_tweet = cv.transform([tweet['text']])
	score = nb.predict(transformed_tweet)
	comps = check_for_companies_and_retweets(transformed_tweet, cv.vocabulary_, symbols)
	return (tweet, score, comps)



if __name__=="__main__":
	cv = get_countvectorizer()
	nb = get_naivebayes()

	newthing = cv.transform(['I am a wonderful goat AAPL MRK, mnst'])
	print newthing.indices
	names = cv.get_feature_names()
	for i in newthing.indices:
		print i, names[i]
	print cv.vocabulary_['aapl']
	print cv.vocabulary_['rt']
	print nb.predict(newthing)
	start = time.time()
	iters = 0
	symbols = get_stock_symbols()
	# for s in symbols:
	# 	print s
	try:
		while True:
			newthing = cv.transform(['I am a truculent goat AAPL MRK, mnst'])
			print check_for_companies(newthing, cv.vocabulary_, symbols)
			print nb.predict(newthing)
			iters += 1
	except KeyboardInterrupt:
		end = time.time() - start
		print iters, 'iters in ', end, 'seconds. Avg', float(iters)/end, 'tweets per second'

	# print get_stock_symbols()
