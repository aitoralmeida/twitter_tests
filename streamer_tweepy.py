# -*- coding: utf-8 -*-
"""
Created on Tue Oct 28 15:08:20 2014

@author: aitor
"""

import tweepy
from tweepy import StreamListener, Stream
import gzip
import time
import datetime
import json

CONSUMER_KEY = '7WVJjUFxjG07hT1boGuS167gJ'
CONSUMER_SECRET ='faY2XRyV4zUO1ScI38T6PsXcVLutOzFgMcAi0VOabgvkq2fXtl'
OAUTH_TOKEN = '2833997469-weiExfPXQ3Mscii21BD5kZHb1srghWzuuLaYLHa'
OAUTH_TOKEN_SECRET = 'B4KYovQBMjnziTeSi1FrkYFOqeKsr4cm76aIrjoknXZUn'

tweets = []
initial_time = time.time()


class StdOutListener(StreamListener):       
 
    def on_status(self, status):
        global tweets, initial_time
        
        elapsed_time = time.time () - initial_time #elapsed secons
        #save the status every 30 mins
        if elapsed_time >= 60 * 30:
            now = datetime.datetime.now()
            file_name = './corpus_new/tweets-%s-%s-%s-%s-%s.txt.gz' % (now.month, now.day, now.hour, now.minute, now.second)
            print('Writing file:', file_name)
            with gzip.open(file_name, 'w') as f:
                for tweet in tweets:
                    f.write(json.dumps(tweet) + '\n')
                    
            tweets = []
            initial_time = time.time()
            
            
        tweets.append(status._json) 
 
        return True
 
    def on_error(self, status_code):
        print('Got an error with status code: ' + str(status_code))
        #sleep 5 mins if an erro occurs
        time.sleep(5 * 60)
        return True # To continue listening
 
    def on_timeout(self):
        print('Timeout...')
        return True # To continue listening
 
if __name__ == '__main__':
    print 'Starting...'
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    
    listener = StdOutListener()
    
    stream = Stream(auth, listener)
    stream.filter(track=['#p2', '#tcot'])
    print 'Done'