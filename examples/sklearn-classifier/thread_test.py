

# General libraries.
import threading, random, Queue, re, time
import numpy as np
import multiprocessing
# import matplotlib.pyplot as plt

# SK-learn libraries for learning.
# from sklearn.pipeline import Pipeline
# from sklearn.neighbors import KNeighborsClassifier
# from sklearn.linear_model import LogisticRegression
# from sklearn.naive_bayes import BernoulliNB
from sklearn.naive_bayes import MultinomialNB
# from sklearn.grid_search import GridSearchCV

# SK-learn libraries for evaluation.
# from sklearn.metrics import confusion_matrix
# from sklearn import metrics
# from sklearn.metrics import classification_report

# SK-learn library for importing the newsgroup data.
from sklearn.datasets import fetch_20newsgroups

# SK-learn libraries for feature extraction from text.
from sklearn.feature_extraction.text import *


categories = ['alt.atheism', 'talk.religion.misc', 'comp.graphics', 'sci.space']
newsgroups_train = fetch_20newsgroups(subset='train',
                                      remove=('headers', 'footers', 'quotes'),
                                      categories=categories)
newsgroups_test = fetch_20newsgroups(subset='test',
                                     remove=('headers', 'footers', 'quotes'),
                                     categories=categories)

num_test = len(newsgroups_test.target)
test_data, test_labels = newsgroups_test.data[num_test/2:], newsgroups_test.target[num_test/2:]
dev_data, dev_labels = newsgroups_test.data[:num_test/2], newsgroups_test.target[:num_test/2]
train_data, train_labels = newsgroups_train.data, newsgroups_train.target

print 'training label shape:', train_labels.shape
print 'test label shape:', test_labels.shape
print 'dev label shape:', dev_labels.shape
print 'labels names:', newsgroups_train.target_names

random.seed(0)

def tweet_preprocessor(s):
    s = s.lower()
    s = re.sub(r'@\w+',' tweetat ',s)
    s = re.sub(r'\d+',' numerictoken ', s) # replace all numbers with " numerictoken " token
    s = re.sub(r'_+', ' ', s)
    return s


cv = CountVectorizer(preprocessor=tweet_preprocessor, ngram_range=(1,1))
ft = cv.fit_transform(train_data)

def generate_bs_tweets(num):
    out = []
    words = cv.get_feature_names()
    for i in xrange(num):
        tweet = []
        chars = 0
        while chars < 140:
            index = random.randint(0, len(words)-1)
            word = words[index]
            tweet.append(word)
            chars += len(word)

        out.append(' '.join(tweet))
        tweet = []

    return out

faketweets = generate_bs_tweets(10000)
faketesttweets = generate_bs_tweets(5000)

fakescores = [random.randint(0,4) for i in xrange(len(faketweets))]
cv = CountVectorizer(preprocessor=tweet_preprocessor, ngram_range=(1,1))
ft = cv.fit_transform(faketweets)
nb = MultinomialNB()
nb.fit(ft, fakescores)

class TweetParser(object):
    def __init__(self, tweet_queue, outlist, name):
        self.t = threading.Thread(target=self.do, args=(tweet_queue, outlist))
        self.t.daemon=True
        print 'starting thread' + str(name)
        self.t.start()
        
    def do(self, tweet_queue, outlist):
        while True:
			now = time.time()
			tweets = tweet_queue.get()
			for tweet in tweets:
				parsed = self.parse_tweet([tweet])
			outlist.append(time.time() - now)
			tweet_queue.task_done()

    def parse_tweet(self, tweets):
        matrix = cv.transform(tweets)
        for i in xrange(1000):
            1515 in matrix.indices
        return nb.predict(matrix)

class TweetParserProcess(multiprocessing.Process):
    def __init__(self, tweet_queue, outlist, name):
    	multiprocessing.Process.__init__(self)
    	self.tweet_queue = tweet_queue
    	self.outlist = outlist
    	self.name = str(name)
        # self.t = Process(target=self.do, args=(tweet_queue, outlist))
        # super(TweetParserProcess, self).__init__()

	def run(self):

		while True:
			now = time.time()
			tweets = self.tweet_queue.get()
			if tweets is None:
				print 'Exiting'
				break
			for tweet in tweets:
				parsed = self.parse_tweet([tweet])
			self.outlist.append(time.time() - now)
			print time.time() - now
			self.tweet_queue.task_done()
		return None


    def parse_tweet(self, tweets):
        matrix = cv.transform(tweets)
        for i in xrange(1000):
            1515 in matrix.indices
        return nb.predict(matrix)

# # myQueue = Queue.Queue()
# myQueue = multiprocessing.JoinableQueue()

# # start = time.time()
# parsers = []
# outlists = [list() for i in range(3)]
# for i, olist in enumerate(outlists):
# 	print i, olist
# 	parser = TweetParserProcess(myQueue, olist, i)
# 	parser.daemon = True
# 	parsers.append(parser)

# for p in parsers:
# 	p.start()
# # print time.time() - start


# start = time.time()
# times = []
# for tweet in faketweets[:-1]:
# 	myQueue.put([tweet])
# 	# pass
# for p in parsers:
# 	myQueue.put(None)
# print time.time() - start
# # myQueue.join()
# total_time = time.time() - start

# suml = 0
# for p in outlists:
# 	print np.mean(p)
# 	suml += len(p)

# print total_time, suml
# print suml / total_time
