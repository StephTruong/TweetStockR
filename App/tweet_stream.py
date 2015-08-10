import json, time, sys
import tweepy
from tweepy import StreamListener
from auth.twitter_credentials import HANK_TWITTER_API_CREDENTIALS ,  STEPH_TWITTER_API_CREDENTIALS, HOWARD_TWITTER_API_CREDENTIALS
from multiprocessing import Process,Pool
from time import sleep
from machine_learning import ml_process_tweet, get_stock_symbols, get_countvectorizer, get_naivebayes, tweet_preprocessor
from databases import getsqlite, get_mongo_conn, insert_into_sentiment_table, upsert_tweet
from aws import get_s3_connection, get_or_create_bucket


def get_twitter_api(twitter_credentials):
    auth = tweepy.OAuthHandler(twitter_credentials['consumer_key'], twitter_credentials['consumer_secret'])
    auth.set_access_token(twitter_credentials['access_token'], twitter_credentials['access_token_secret'])
    
    return tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True),auth


class SListener(StreamListener):

    def __init__(self, api, fprefix = 'streamer'):
        
        self.api = api
        self.fprefix = fprefix
        # self.delout  = open('output/delete.txt', 'a')
        print "init done for:" +fprefix
        self.mongo = get_mongo_conn()
        self.mongocoll = self.mongo['tweetstock'].temptweets
        self.sql = getsqlite()
        self.cursor = self.sql.cursor()
        self.bucket = get_or_create_bucket('tweetstock-tweets')
        self.symbols = get_stock_symbols()
        self.cv = get_countvectorizer()
        self.nb = get_naivebayes()
        self.count = 0
        self.start_time = time.time()
        
    def on_data(self, data):

        if  'in_reply_to_status' in data:
            self.on_status(data)
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print warning['message']
            return False
        
    def on_status(self, status):
        tweet = json.loads(status)
        if not upsert_tweet(self.mongocoll, tweet, self.bucket):
            processed = ml_process_tweet(tweet, self.cv, self.nb, self.symbols)
            insert_into_sentiment_table(self.sql, self.cursor, processed)
        self.count += 1
        if self.count % 10 == 0:
            print self.fprefix, self.count, 'tweets in', time.time() - self.start_time, 'seconds', float(self.count) / (time.time() - self.start_time)
        return



    def on_limit(self, track):
        sys.stderr.write(str(track) + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return 
    



## authentication



def main():
    
    # with open('auth/twitter_credentials.json','rb') as au:
    #     creds = json.load(au)
    # HANK_TWITTER_API_CREDENTIALS = creds['HANK_TWITTER_API_CREDENTIALS']
    # STEPH_TWITTER_API_CREDENTIALS = creds['STEPH_TWITTER_API_CREDENTIALS']
    # HOWARD_TWITTER_API_CREDENTIALS = creds['HOWARD_TWITTER_API_CREDENTIALS']

    apiHank, authHank     = get_twitter_api(HANK_TWITTER_API_CREDENTIALS)
    apiSteph, authSteph    =  get_twitter_api(STEPH_TWITTER_API_CREDENTIALS)
    apiHoward, authHoward    =  get_twitter_api(HOWARD_TWITTER_API_CREDENTIALS)

    # we need to gather three streams:
    #- one unfiltered containing all tweets from firehose
    #- one filtered with high flyers stock (e.g. Google, Yahoo, ...)
    #- one filtered with low flyers stock
    def runHF():
        trackHF = [u"#Apple",u"#Google",u"#Yahoo"]
        listenHF = SListener(apiHank,'HighFreq')
        streamHF = tweepy.Stream(authHank, listenHF)
        streamHF.filter(track=trackHF,languages=['en'])
        
    def runLF():
        trackLF= [u'#Alcoa',u'#ProcterGamble',u'#HBO']
        listenLF = SListener(apiSteph,'LowFreq' )
        streamLF = tweepy.Stream(authSteph, listenLF)
        streamLF.filter(track=trackLF,languages=['en'])

    def runAll():
        listenAll = SListener(apiHoward,'All' )
        streamAll = tweepy.Stream(authHoward, listenAll)
        streamAll.sample(languages=['en'])

    print "Streaming started..."
    # apps=[runHF,runLF,runAll]
    # processes={}
    # n=0
   
    # for app in apps:
    #     instance = app()
    #     p = Process(target=instance.start_listener)
    #     p.start()
    #     processes[n] = (p,a) # Keep the process and the app to monitor or restart
    #     n += 1

    # while len(processes) > 0:
    #     for n in processes.keys():
    #         (p,a) = processes[n]
    #         sleep(0.5)
    #         if p.exitcode is None and not p.is_alive(): # Not finished and not running
    #             print "alive"
    #              # Do your error handling and restarting here assigning the new process to processes[n]
    #         elif p.exitcode < 0:
    #             print ('Process Ended with an error or a terminate', a)
    #             # Handle this either by restarting or delete the entry so it is removed from list as for else
    #         else:
    #             print (a, 'finished')
    #             p.join() # Allow tidyup
    #             del processes[n] # Removed finished items from the dictionary 
    #             # When none are left then loop will end

    
    try: 
        while True:
            try:
                #not sure how to pass keyword argument so im going brute when i call process class.
                pHF= Process(target=runHF)
                pLF= Process(target=runLF)
                pAll= Process(target=runAll)
                pHF.daemon = True
                pLF.daemon = True
                pAll.daemon = True
                pHF.start()
                pLF.start()
                pAll.start()
                pHF.join()
                pLF.join()
                pAll.join()
            except Exception:
                print "error!"
                pHF.join()
                pLF.join()
                pAll.join()
    except KeyboardInterrupt:
        pHF.terminate()
        pLF.terminate()
        pAll.terminate()


if __name__ == '__main__':
    main()
