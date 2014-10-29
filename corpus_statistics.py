# -*- coding: utf-8 -*-
"""
Created on Fri Oct 17 11:07:36 2014

@author: aitor
"""
from glob import glob
import json
import gzip

CORPUS = 'corpus' #corpus, corpus_lite

def total_users():
    users = set()
    total_retweets = 0
    total_tweets = 0
    for i, file_name in enumerate(glob('./' + CORPUS + '/*.txt.gz')):
        print 'Processing %i of %i files' % (i, len(glob('./' + CORPUS + '/*.txt.gz')))
        with gzip.open(file_name, 'r') as f:
            for line in f:
                try:
                    tweet = json.loads(line)
                except:
                    continue
            
                try:
                    user_id = tweet['user']['id']
                    users.add(user_id)
                except:
                    continue
                
                total_tweets += 1
                
                if tweet.has_key('retweeted_status'):
                    total_retweets += 1
    
    print '\nRESULTS:'                
    print ' - Total users:', len(users)
    print ' - Total tweets:', total_tweets
    print ' - Total retweets:', total_retweets
    
if __name__=='__main__':  
    print 'Starting...'
    
    total_users()
    
    print 'DONE'
    