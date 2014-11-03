# -*- coding: utf-8 -*-
"""
Created on Fri Oct 17 11:07:36 2014

@author: aitor
"""
from glob import glob
import json
import gzip
from pandas import Series

CORPUS = 'corpus' #corpus, corpus_lite

def total_users():
    users = set()
    total_retweets = 0
    total_tweets = 0
    total_mentions = 0
    total_replies = 0
    tweet_times = {} # {'1230' : 10, '1200': 4}
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
                    
                total_mentions += len([mention['id_str'] for mention in tweet['entities']['user_mentions']])
                
                if tweet['in_reply_to_user_id']:                
                    total_replies += 1
                    
                
                post_datetime = tweet['created_at']
                time = post_datetime.split(' ')[3]
                hour = time.split(':')[0]
                mins = time.split(':')[1]
                if int(mins[0]) < 3:
                    mins = '00'
                else:
                    mins = '30'
                period = hour+mins
                if tweet_times.has_key(period):
                    tweet_times[period] += 1
                else:
                    tweet_times[period] = 1
                
                
    
    print '\nRESULTS:'                
    print ' - Total users:', len(users)
    print ' - Total tweets:', total_tweets
    print ' - Original tweets: %s(%s%%)' % (total_tweets - total_retweets, ((total_tweets - total_retweets) * 1.0 / total_tweets) * 100.0)
    print ' - Total replies: %s(%s%%)' % (total_replies, (total_replies * 1.0 / total_tweets) * 100.0)
    print ' - Total retweets: %s(%s%%)' % (total_retweets, (total_retweets * 1.0 / total_tweets) * 100.0)
    print ' - Total mentions: %s' % (total_mentions)
    print tweet_times
    tweet_series = Series(tweet_times)
    tweet_series.plot()
    
if __name__=='__main__':  
    print 'Starting...'
    
    total_users()
    
    print 'DONE'
    