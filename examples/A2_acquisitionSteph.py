import json, time, sys
import tweepy
from tweepy import StreamListener

class SListener(StreamListener):

    def __init__(self, api = None, fprefix = 'streamer',ftrack=None):
        
        self.api = api or API()
        self.counterBoth = 0
        self.counter1 = 0 
        self.counter2 = 0
        self.ftrack = ftrack
        self.fprefix = fprefix
        self.outputBoth  = open(fprefix + ftrack[0]+'&'+ftrack[1]+'.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
        self.output1  = open(fprefix + ftrack[0]+'Only.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
        self.output2  = open(fprefix + ftrack[1]+'Only.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
        self.delout  = open('delete.txt', 'a')
        print "init done"
        
    def on_data(self, data):
        print "on data"
        if  'in_reply_to_status' in data:
            self.on_statusBoth(data)
            if (self.ftrack[0] in data and self.ftrack[1] not in data):
                print "child1"
                self.on_status1(data)
            elif (self.ftrack[1] in data and self.ftrack[0] not in data):
                print "child2"
                self.on_status2(data)

        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print warning['message']
            return false
        

    def on_statusBoth(self, status):
        print "onstatusboth",self.counterBoth
        self.outputBoth.write(status + "\n")

        self.counterBoth += 1

        if self.counterBoth>= 200:
            self.outputBoth.close()
            self.outputBoth  = open(self.fprefix + self.ftrack[0]+'&'+self.ftrack[1]+'.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
            self.counterBoth = 0
        print "onstatusBoth done"
        return

    def on_status1(self, status):
        print "onstatus1",self.counter1

        self.output1.write(status + "\n")

        self.counter1 += 1

        if self.counter1>= 200:
            self.output1.close()
            self.output1 =  open(self.fprefix + self.ftrack[0]+'Only.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')

            self.counter1 = 0
        return

    def on_status2(self, status):
        print "onstatus2",self.counter2

        self.output2.write(status + "\n")

        self.counter2 += 1

        if self.counter2>= 200:
            self.output2.close()
            self.output2 =  open(self.fprefix+self.ftrack[1]+'Only.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
            self.counter2 = 0
        return

    def on_limit(self, track):
        sys.stderr.write(track + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return 
    



## authentication
consumer_key = <insert key>
consumer_secret = <insert key>

access_token = <insert key>
access_token_secret = <insert key>
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api      = tweepy.API(auth)

def main():
    track = ['#NBAFinals2015', '#Warriors']
 
    listen = SListener(api,'A2',track)
    stream = tweepy.Stream(auth, listen)

    print "Streaming started..."

    try: 
        stream.filter(track = track)
    except:
        print "error!"
        stream.disconnect()

if __name__ == '__main__':
    main()