# -*- coding: utf-8 -*-
"""
Created on Tue Oct 28 15:08:20 2014

@author: aitor
"""

import sys
import random
import tweepy
from tweepy import StreamListener, Stream
import gzip
import time
import datetime
import json

try:
    from config import OAUTH_TOKEN, OAUTH_TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET
except ImportError:
    print "config.py not found. Run:"
    print ""
    print " $ cp config.py.dist config.py "
    print ""
    print "Edit the config.py file, and run this command again"
    sys.exit(-1)

tweets = []
initial_time = time.time()


class StdOutListener(StreamListener):       
 
    def on_data(self, raw_data):
        global tweets, initial_time
        
        elapsed_time = time.time () - initial_time #elapsed secons
        #save the status every 30 mins
        if elapsed_time >= 60 * 30:
            now = datetime.datetime.now()
            file_name = './corpus_new/tweets-%s-%s-%s-%s-%s.txt.gz' % (now.month, now.day, now.hour, now.minute, now.second)
            print 'Writing file:', file_name
            with gzip.open(file_name, 'w') as f:
                for tweet in tweets:
                    f.write(json.dumps(tweet) + '\n')
                    
            tweets = []
            initial_time = time.time()
        
        try:
            data = json.loads(raw_data)
            tweets.append(data) 
        except:
            now = datetime.datetime.now()
            print '(%s %s:%s)Invalid json data: %s' % (now.day, now.hour, now.minute, raw_data)
             
        return True
 
    def on_error(self, status_code):
        now = datetime.datetime.now()
        print '(%s %s:%s)Got an error with status code: %s' % (now.day, now.hour, now.minute, status_code)
        #sleep 5 mins if an error occurs
        time.sleep(5 * 60)
        return True # To continue listening
 
    def on_timeout(self):
        print 'Timeout...' 
        return True # To continue listening
        
 
if __name__ == '__main__':
    print 'Starting...'
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    
    listener = StdOutListener()
    
    query = ['#p2', '#tcot', '#gov', '#dem', '#dems', '#gop']
    random.shuffle(query)
    stream = Stream(auth, listener)
    stream.filter(track=query)

    print 'Done'
