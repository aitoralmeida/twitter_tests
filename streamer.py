# -*- coding: utf-8 -*-
"""
@author: aitor
"""

import twitter
import woe_ids
import datetime
import json
import time


CONSUMER_KEY = '7WVJjUFxjG07hT1boGuS167gJ'
CONSUMER_SECRET ='faY2XRyV4zUO1ScI38T6PsXcVLutOzFgMcAi0VOabgvkq2fXtl'
OAUTH_TOKEN = '2833997469-weiExfPXQ3Mscii21BD5kZHb1srghWzuuLaYLHa'
OAUTH_TOKEN_SECRET = 'B4KYovQBMjnziTeSi1FrkYFOqeKsr4cm76aIrjoknXZUn'

auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET,
                           CONSUMER_KEY, CONSUMER_SECRET)

twitter_api = twitter.Twitter(auth=auth)


def get_trends(woe_id=1):
# Returns the trends for an specific location. Returns the global trends by default
# woe_id = the country id
# API: https://dev.twitter.com/rest/reference/get/trends/place
# Country ids: 
#  https://developer.yahoo.com/geo/geoplanet/
#  https://developer.yahoo.com/geo/geoplanet/guide/yql-tables.html#geo-countries
    results = twitter_api.trends.place(_id=woe_id)[0]['trends']
    trends = [result['name'] for result in results]
    return trends
    

def do_query(q, count = 100, total_iter = 1, since_id = -1):
# total_iter = 0 to retrieve all the results
# count = The number of tweets to return per page, up to a maximum of 100. 
#        Defaults to 15. This was formerly the “rpp” parameter in the old 
#        Search API
# total_iter * count = total tweets to retrieve
#https://dev.twitter.com/rest/reference/get/search/tweets    
    if since_id == -1:
        results = twitter_api.search.tweets(q=q, count=count)
    else:
        results = twitter_api.search.tweets(q=q, count=count, since_id=since_id) 
       
    statuses = results['statuses']
    last_id = results['search_metadata']['max_id']
    
    if total_iter > 1:
        for i in range(total_iter-1):
            #taken from http://nbviewer.ipython.org/github/ptwobrussell/Mining-the-Social-Web-2nd-Edition/blob/master/ipynb/Chapter%201%20-%20Mining%20Twitter.ipynb
            try:
                 next_results = results['search_metadata']['next_results']
            except KeyError:
                 break
             
            kwargs = dict([ kv.split('=') for kv in next_results[1:].split("&") ])
            search_results = twitter_api.search.tweets(**kwargs)
            statuses += search_results['statuses']
            last_id = search_results['search_metadata']['max_id']
    elif total_iter == 0:
        cont = True
        while cont:
            try:
                next_results = results['search_metadata']['next_results']
            except KeyError:
                cont = False

             
            kwargs = dict([ kv.split('=') for kv in next_results[1:].split("&") ])
            try:
                search_results = twitter_api.search.tweets(**kwargs)
                statuses += search_results['statuses']   
                last_id = search_results['search_metadata']['max_id']        
            except: # max requests reached
                cont = False

    return statuses, last_id
    
def monitorize_tweets(terms):
# Continuously monitorizes a list of terms
    query = ' OR '.join(terms)
    print ' - Monitorizing the terms:', query    
    last_id = -1
        
    while True:
        print '    - Last id:', last_id
        tweets, last_id = do_query(query, 100, 0, last_id)
        _save_tweets(tweets)
        print '    - Sleeping for 15 mins'
        time.sleep((15 * 60) + 1)
        
def _save_tweets(tweets):
# Saves tweets to a file, one tweet per line
    now = datetime.datetime.now()
    month = now.month
    day = now.day
    hour = now.hour
    minute = now.minute
    file_name = './corpus/tweets-%s-%s-%s-%s.txt' % (month, day, hour, minute)
    print '    - Saving file:', file_name
    with open(file_name, 'w') as f:
        for tweet in tweets:
            f.write(json.dumps(tweet) + '\n')
            
if __name__=='__main__':     
    print 'STARTING...'
    
    # get the trends for spain     
    print get_trends(woe_ids.countries['Spain'])    
    
    # get 200 tweets for a tag    
    tweets, __ = do_query('#p2 OR #tcot', 100, 2)   
    print 'Total tweets recovered:', len(tweets)
    for t in [x['text'] for x in tweets]:
        try:
            print ' - ', t
        except:
            pass

    # monitorize tweets, this continues for ever
    monitorize_tweets(['#p2', '#tcot'])
    
    print 'DONE'



