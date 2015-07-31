import json, time, sys
import tweepy
from tweepy import StreamListener
from auth.credentials import HANK_TWITTER_API_CREDENTIALS ,  STEPH_TWITTER_API_CREDENTIALS
from multiprocessing import Process,Pool

def get_twitter_api(twitter_credentials = HANK_TWITTER_API_CREDENTIALS):
    auth = tweepy.OAuthHandler(twitter_credentials['consumer_key'], twitter_credentials['consumer_secret'])
    auth.set_access_token(twitter_credentials['access_token'], twitter_credentials['access_token_secret'])
    
    return tweepy.API(auth_handler=auth,wait_on_rate_limit=True,wait_on_rate_limit_notify=True),auth


class SListener(StreamListener):

    def __init__(self, api = None, fprefix = 'streamer'):
        
        self.api = api or API()
        self.fprefix = fprefix
        self.output  = open("output/"+fprefix + '.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
        self.counter=0
        self.delout  = open('output/delete.txt', 'a')
        print "init done"
        
    def on_data(self, data):

        if  'in_reply_to_status' in data:
            self.on_status(data)


        elif 'limit' in data:
            print "limit" +data
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print warning['message']
            return false
        

    def on_status(self, status):
        print "onstatus",self.counter
        self.output.write(status + "\n")

        self.counter += 1

        if self.counter>= 20000:
            self.output.close()
            self.output  = open("output/"+self.fprefix +'.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
            self.counter = 0
        print "onstatus done"
        return



    def on_limit(self, track):
        print track
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

apiHank, authHank     = get_twitter_api(HANK_TWITTER_API_CREDENTIALS)
apiSteph, authSteph    =  get_twitter_api(STEPH_TWITTER_API_CREDENTIALS)

# we need to gather three streams:
#- one unfiltered containing all tweets from firehose
#- one filtered with high flyers stock (e.g. Google, Yahoo, ...)
#- one filtered with low flyers stock
def runHF(): 
    trackHF = [u"#Apple",u"#Google",u"#Yahoo"]
    listenHF = SListener(apiHank,'HighFreq')
    streamHF = tweepy.Stream(authHank, listenHF)
    streamHF.filter(track=trackHF)
    
def runLF():
    trackLF= [u'#Alcoa',u'#ProcterGamble',u'#HBO']
    listenLF = SListener(apiSteph,'LowFreq' )
    streamLF = tweepy.Stream(authSteph, listenLF)
    streamLF.filter(track=trackLF)

def main():
    

    print "Streaming started..."

    try: 
        #not sure how to pass keyword argument so im going brute whn i call process class.
        pHF= Process(target=runHF)
        pLF= Process(target=runLF)
        pHF.start()
        pLF.start()
        pHF.join()
        pLF.join()
    except:
        print "error!"
        pHF.join()
        pLF.join()

if __name__ == '__main__':
    main()