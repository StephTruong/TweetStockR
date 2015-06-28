__author__ = 'HowardW'
import sys
import tweepy
import urllib
# import datetime
import signal
import json
import partitions
import time

# Don't forget to install tweepy
# pip install tweepy

consumer_key = "Type Here"
consumer_secret = "Type Here"

access_token = "Type Here"
access_token_secret = "Type Here"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

class TweetSerializer:
    def __init__(self):
        self.out = None
        self.first = True
        self.count = 0

    def start(self):
        self.count += 1
        fname = "tweets-"+str(self.count)+".json"
        self.out = open(fname,"w")
        self.out.write("[\n")
        self.first = True

    def end(self):
        if self.out is not None:
            self.out.write("\n]\n")
            self.out.close()
        self.out = None

    def write(self,tweet):
        if not self.first:
            self.out.write(",\n")
        self.first = False
        self.out.write(json.dumps(tweet._json).encode('utf8'))

# The following addresses the "Chunking" and "Interrupts" sections from the W205 live class assignment using Tweepy
# access the Twitter API with Python.
 
# init serializer
writer = TweetSerializer()
 
search_text = sys.argv[1]  # get search input from command line (i.e. $ python search.py <search term>)
#search_text = "#NBAFinals2015"

### KEYBOARD INTERRUPT HANDLER
# signal looks for keyboard interrupt (in unix, ctrl + c) and executes function interrupt()
# this is necessary to prevent malformed output files.
#
# if you use a keyboard interrupt while the program is running but DID NOT define
# a function to handle it (try it!), the current file would instantly close without
# adding the final bracket "]", which will cause problems
# when reading these files into memory later on
def interrupt(signum, frame):
    print "It's quitting time! Closing open file."
    writer.end() # closes end bracket in active file before exiting program
    exit(1) # exits python
signal.signal(signal.SIGINT, interrupt) # start keyboard interrupt handler
###
 
written = 0 # num of tweets written to current file
max_single_file = 5000
writer.start() # open new file

c = tweepy.Cursor(api.search,
                           q=search_text,
                           since= '2015-06-17', until= '2015-06-18').items()
while True:
    try:
        tweet = c.next()
        if written < max_single_file:
            written += 1 # add 1 to num of tweets written to current file
            writer.write(tweet)
        else:
            writer.end() # close file
            writer.start() # open new file
            writer.write(tweet)
            written = 1 # reset num of tweets written to current file
    except tweepy.TweepError:
        time.sleep(60 * 15)
        continue
    except StopIteration:
        break
writer.end()