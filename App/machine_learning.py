import pickle
import re
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.externals import joblib


def tweet_preprocessor(s):
    s = s.lower()
    s = re.sub(r'@\w+',' tweetat ',s)
    s = re.sub(r'\d+',' numerictoken ', s) # replace all numbers with " numerictoken " token
    s = re.sub(r'_+', ' ', s)
    s = re.sub(r'(http\S+)', ' URL ', s)
    return s


def get_cv(fname = 'count_vectorizer.pickle'):
	return joblib.load(fname)


if __name__=="__main__":
	cv = get_cv()

	newthing = cv.transform(['I am a happy goat AAPL MRK, mnst'])
	print newthing.indices
	names = cv.get_feature_names()
	for i in newthing.indices:
		print i, names[i]
	print cv.vocabulary_['aapl']
