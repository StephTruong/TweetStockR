import pickle
import re
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.externals import joblib
import time
import csv
from databases import insert_into_sentiment_table


def get_stock_symbols(fname = 'picklejar/tickers.pickle'):
	with open(fname, 'rb') as f:
		tickers = pickle.load(f)
	out = []
	for tickerlist in tickers:
		for ticker in tickerlist.split(','):
			out.append(ticker.split(':')[1].lower())
	return out

def get_countvectorizer(fname = 'picklejar/count_vectorizer-081215.pickle'):
	return joblib.load(fname)

def get_naivebayes(fname='picklejar/naive_bayes-081215.pickle'):
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

def train_sentiment_model():
	with open('../training.1600000.processed.noemoticon.csv','rb') as newdata:
		data=[]
		scores=[]
		failed=0
		reader = csv.reader(newdata)
		for line in reader:
			try:
				data.append(line[5].decode('utf-8'))
				scores.append(int(line[0]))
			except:
				failed += 1
	print failed
	print len(data)
	print len(scores)

	# data.append()

	np.random.seed(100)
	sample_ids = np.random.choice(len(data), int(len(data) * .9), replace=False)

	train_data, train_labels = np.array(data)[sample_ids], np.array(scores)[sample_ids]
	dev_data, dev_labels = np.array(data)[~sample_ids], np.array(scores)[~sample_ids]

	cv = CountVectorizer(min_df=1, preprocessor = tweet_preprocessor)

	import pickle
	
	symbols = get_stock_symbols()

	string = ' '.join(symbols)
	# print data[-3:]
	data.append(string)
	print data[-3:]

	print 'fitting'
	formatted_data = cv.fit_transform(np.array(data))

	joblib.dump(cv, 'picklejar/count_vectorizer-081215.pickle')

	print 'training'
	train_transformed = cv.transform(train_data)
	nb = MultinomialNB()
	nb.fit(train_transformed, train_labels)
	
	joblib.dump(nb, 'picklejar/naive_bayes-081215.pickle')


if __name__=="__main__":
	pass
	# train_sentiment_model()

