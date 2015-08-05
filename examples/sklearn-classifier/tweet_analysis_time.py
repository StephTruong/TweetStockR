import time, re, csv, numpy as np

# ml packages
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.externals import joblib

def get_training_data(fin):
	data, scores = [],[]
	with open(fin, 'rb') as f:
		reader = csv.reader(f)
		for line in reader:
			scores.append(line[0])
			data.append(line[1])
		return (data, scores)

def tweet_preprocessor(s):
    s = s.lower()
    s = re.sub(r'@\w+',' tweetat ',s)
    s = re.sub(r'\d+',' numerictoken ', s) # replace all numbers with " numerictoken " token
    s = re.sub(r'_+', ' ', s)
    s = re.sub(r'(http\S+)', ' URL ', s)
    return s

def main():
	data, scores = get_training_data('classifiedtweets.txt')
	# data.append()

	np.random.seed(100)
	sample_ids = np.random.choice(len(data), int(len(data) * .5), replace=False)

	train_data, train_labels = np.array(data)[sample_ids], np.array(scores)[sample_ids]
	dev_data, dev_labels = np.array(data)[~sample_ids], np.array(scores)[~sample_ids]


	cv = CountVectorizer(min_df=1, preprocessor = tweet_preprocessor)

	tickerfiles = ['../../App/tickers/tickerlist1.csv', '../../App/tickers/tickerlist2.csv', '../../App/tickers/tickerlist3.csv']

	symbols = []
	for f in tickerfiles:
		with open(f, 'rb') as fin:
			for line in fin:
				symbols.append(line.split(',')[0])
	print len(symbols)

	string = ' '.join(symbols)
	# print data[-3:]
	data.append(string)
	# print data[-3:]

	formatted_data = cv.fit_transform(np.array(data))

	joblib.dump(cv, 'count_vectorizer.pickle')

	# train_transformed = cv.fit_transform(train_data)
	# nb = MultinomialNB()
	# nb.fit(train_transformed, train_labels)
	
	# transform_times = []
	# for d in dev_data:

	# 	now = time.time()
	# 	cv.transform(d)
	# 	transform_times.append(time.time() - now)


	# predict_transform_times = []
	# for d in dev_data:
	# 	now = time.time()
	# 	try:
	# 		a = cv.transform(d)
	# 		b = nb.predict(a)
	# 		predict_transform_times.append(time.time() - now)
	# 	except Exception:
	# 		pass


	# processed = 0
	# start = time.time()
	# for d in dev_data:
	# 	if time.time() - start < 1.0:
	# 		try:
	# 			d = np.array([d])
	# 			a = cv.transform(d)
	# 			print d, a, '\n'
	# 			b = nb.predict(a)
	# 			# print b
	# 			processed += 1
	# 		except Exception:
	# 			pass

	# print train_transformed.shape
	# print "Transform:", np.mean(transform_times)
	# print "Predict transform:", np.mean(predict_transform_times)
	# print "Tweets processed in 1 second:", processed



if __name__=="__main__":
	main()
