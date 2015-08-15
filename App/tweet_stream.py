import logging
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
        logging.info("init done for:" +fprefix)
        self.mongo = get_mongo_conn()
        self.upsertcoll = self.mongo['tweetstock'].temptweets
        self.sentimentcoll = self.mongo['tweetstock'].sentiment
        # self.sql = getsqlite('tweets.sqlite3')
        # self.cursor = self.sql.cursor()
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
        if not upsert_tweet(self.upsertcoll, tweet, self.bucket):
            processed = ml_process_tweet(tweet, self.cv, self.nb, self.symbols)
            insert_into_sentiment_coll(self.sentimentcoll, processed)
            # insert_into_sentiment_table(self.sql, self.cursor, processed)
        self.count += 1
        if self.count % 50 == 0:
            print self.fprefix, self.count, 'tweets in', time.time() - self.start_time, 'seconds', float(self.count) / (time.time() - self.start_time)
        return


    def on_limit(self, track):
        sys.stderr.write(str(track) + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        print 'sleeping'
        time.sleep(60)
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return 


## authentication
def main():
    

    apiHank, authHank     = get_twitter_api(HANK_TWITTER_API_CREDENTIALS)
    apiSteph, authSteph    =  get_twitter_api(STEPH_TWITTER_API_CREDENTIALS)
    apiHoward, authHoward    =  get_twitter_api(HOWARD_TWITTER_API_CREDENTIALS)

    # we need to gather three streams:
    #- one unfiltered containing all tweets from firehose
    #- one filtered with high flyers stock (e.g. Google, Yahoo, ...)
    #- one filtered with low flyers stock
    def runHF():
        lastsleep = time.time()
        stdsleep=1
        while True:
            try:
                print 'HF listener init'
                trackHF = [u"Apple",u"iphone",u"iOs",u"itunes",u"macbook",u"ipad",u"@apple",u"#apple",u"$AAPL",u"Berkshire Hathaway",u"Warren Buffet",u"@Berkshire Hathaway",u"@BerkshireHathaway",u"@warrenbuffet",u"#BerkshireHathaway",u"#BRK/B",u"Exxon",u"@exxon",u"#exxon",u"#XOM",u"WellsFargo",u"@WellsFargo",u"#WellsFargo",u"#WFC",u"johnson&johnson",u"@JNJNews",u"@JNJCares",u"#JNJ",u"facebook",u"@facebook",u"#facebook",u"#FB",u"urban outfitters",u"@UrbanOutfitters",u"#UrbanOutfitters",u"#URBN",u"Owens-illinois",u"#OwensIllinois",u"#OI",u"Joy Global",u"@JoyGlobalInc",u"#JOY",u"Flir system", u"@SeeAtNight", u"#FLIR"]
                listenHF = SListener(apiHank,'HighFreq')
                streamHF = tweepy.Stream(authHank, listenHF)
                print 'HF listener stream'
                lastsleep=time.time()
                streamHF.filter(track=trackHF,languages=['en'])
            except Exception, e:
                print e
                if e != "database error: collection dropped between getMore calls":
                    if stdsleep <= 300:
                        stdsleep *= 2
                    else:
                        stdsleep=300
                    logging.warning(e)
                    print 'Error in HF! Sleeping for', stdsleep
                    if time.time() - lastsleep > 360:
                        stdsleep = 4
                    time.sleep(stdsleep)
            except KeyboardInterrupt:
                break
                raise Exception('Streaming Cancelled')
        
    def runLF():
        lastsleep=time.time()
        stdsleep=1
        while True:
            try:
                print 'LF listener init'
                trackLF= [u"Staples",u"@staples",u"#staples",u"#SPLS",u"newscorp",u"@newscorp",u"#NWSA",u"michael kors",u"@MichaelKors",u"#KORS",u"mattel",u"@mattel",u"#MAT",u"urban outfitters",u"@UrbanOutfitters",u"#UrbanOutfitters",u"#URBN",u"Owens-illinois",u"#OwensIllinois",u"#OI",u"Joy Global",u"@JoyGlobalInc",u"#JOY",u"Flir system", u"@SeeAtNight", u"#FLIR",u"Google",u"@google",u"#alphabet" "#google",u"#GOOGL",u"Microsoft",u"Windows",u"@microsoft",u"#microsoft",u"#MSFT"]
                listenLF = SListener(apiSteph,'LowFreq' )
                streamLF = tweepy.Stream(authSteph, listenLF)
                print 'lf listener stream'
                lastsleep=time.time()
                streamLF.filter(track=trackLF,languages=['en'])
            except Exception, e:
                print e
                if e != "database error: collection dropped between getMore calls":
                    if stdsleep <= 300:
                        stdsleep *= 2
                    else:
                        stdsleep=300
                    logging.warning(e)
                    print 'Error in LF! Sleeping for', stdsleep
                    if time.time() - lastsleep > 360:
                        stdsleep = 4
                    time.sleep(stdsleep)
            except KeyboardInterrupt:
                break
                raise Exception('Streaming Cancelled')

    def runAll():
        lastsleep=time.time()
        stdsleep=1
        while True:
            try:
                print 'Sample listener init'
                listenAll = SListener(apiHoward,'All' )
                streamAll = tweepy.Stream(authHoward, listenAll)
                print 'sample stream'
                lastsleep=time.time()
                streamAll.sample(languages=['en'])
            except Exception, e:
                print e
                if e != "database error: collection dropped between getMore calls":
                    if stdsleep <= 300:
                        stdsleep *= 2
                    else:
                        stdsleep=300
                    logging.warning(e)
                    print 'Error in sample! Sleeping for', stdsleep
                    if time.time() - lastsleep > 360:
                        stdsleep = 4
                    time.sleep(stdsleep)
            except KeyboardInterrupt:
                break
                raise Exception('Streaming Cancelled')

    logging.info("Streaming started...")
    # apps=[runHF,runLF,runAll]
    # processes={}
    # n=0

    
    try: 
        procs = [Process(target=run) for run in [runHF, runLF, runAll]]
        for proc in procs:
            proc.daemon = True
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
    except Exception, e:
        logging.warning("stream error!", e)
        for proc in procs:
            proc.join()
    except KeyboardInterrupt:
        for proc in procs:
            proc.terminate()


if __name__ == '__main__':
    logging.basicConfig(filename='stream.log', level=logging.DEBUG)
    main()

