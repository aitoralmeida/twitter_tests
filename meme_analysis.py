# -*- coding: utf-8 -*-
"""
Created on Tue Nov 04 09:49:31 2014

We consider both tags and urls as memes

@author: aitor
"""

from glob import glob
import json
import gzip
import pandas as pd
import networkx as nx

CORPUS = 'corpus' #corpus, corpus_lite
DATA = 'data' #data output folder
SEED = ['p2', 'tcot', 'gov', 'dem', 'dems', 'gop']
FILTER_QUANTILE = 0.99 # we only use the 1% most relevant memes

def count_meme_appearances(): 
    # Counts how many times a meme appears and which are the injection points
    print 'Counting meme appearances'    
    total_tags = 0
    total_urls = 0
    meme_days = {} #{'meme' : {'month' : {'day' : {'id' : 14, 'id2' : 1}}}
    url_count = {} # {'id': 24}
    retweet_count = {} # only those retweets with URLs {id_from : {id_to : 13}}
    for i, file_name in enumerate(glob('./' + CORPUS + '/*.txt.gz')):
        if i % 10 == 0:
            print 'Processing %i of %i files' % (i, len(glob('./' + CORPUS + '/*.txt.gz')))
        with gzip.open(file_name, 'r') as f:
            for line in f:
                try:
                    tweet = json.loads(line)
                except:
                    continue
            
                try:
                    user_id = tweet['user']['id']
                    post_datetime = tweet['created_at']  
                    tokens = post_datetime.split(' ')
                    month = tokens[1]
                    day = tokens[2]                                  
                except:
                    continue
                
                from_id = ''
                if tweet.has_key('retweeted_status'):
                    from_id = tweet['retweeted_status']['user']['id'] 
                    original_tweet = tweet['retweeted_status']
                    original_urls = [m['expanded_url'] for m in original_tweet['entities']['urls']]
                    if url_count.has_key(from_id):
                        url_count[from_id] += len(original_urls)
                    else:
                        url_count[from_id] = len(original_urls)                    
                    
                    if len(original_urls) > 0:
                        if not retweet_count.has_key(from_id):
                            retweet_count[from_id] = {user_id : len(original_urls)}
                        elif not retweet_count[from_id].has_key(user_id):
                            retweet_count[from_id][user_id] = len(original_urls)
                        else:
                            retweet_count[from_id][user_id] += len(original_urls)
                        
                
                try:
                    tags = [x['text'] for x in tweet['entities']['hashtags']]
                    for t in tags:
                        tag = t.lower()
                        if not tag in SEED:                                             
                            _add_meme(meme_days, tag, month, day, user_id, from_id)
                            total_tags += 1
                except:
                    pass
                
                try:
                    urls = [m['expanded_url'] for m in tweet['entities']['urls']]
                    if not tweet.has_key('retweeted_status'):
                        if url_count.has_key(user_id):
                            url_count[user_id] += len(urls)
                        else:
                            url_count[user_id] = len(urls)
                        
                    for url in urls:
                        _add_meme(meme_days, url, month, day, user_id, from_id) 
                        total_urls += 1
                except:
                    pass
    print 'Total tags: %s' % (total_tags)
    print 'Total URLs: %s' % (total_urls)
    print 'Writing file...'                    
    json.dump(meme_days, open('./' + DATA + '/meme_count.json', 'w'), indent=2)
    json.dump(url_count, open('./' + DATA + '/url_count.json', 'w'), indent=2)
    json.dump(retweet_count, open('./' + DATA + '/retweet_count.json', 'w'), indent=2)
    
def count_meme_id_diversity():
    # calculates the diversity of each meme, both per day and global.
    print 'Calculating meme diversity'
    print ' - Loading file'
    meme_count = json.load(open('./' + DATA + '/meme_count.json', 'r'))
    meme_diversity = {}
    for i, meme in enumerate(meme_count):     
        if i % 500 == 0:
            print 'Processing %s of %s meme' % (i, len(meme_count))    
            
        total_total_msgs = 0
        total_different_ids = set()
                
        for month in meme_count[meme]:
            for day in meme_count[meme][month]:
                total_msgs = 0
                different_ids = set ()
                for id_user in meme_count[meme][month][day]:
                    if id_user != 'total':
                        total_msgs += meme_count[meme][month][day][id_user]['total'] 
                        different_ids.add(id_user)
                        total_different_ids.add(id_user)

                total_total_msgs += total_msgs
                
                diversity = (len(different_ids) * 100.0) / total_msgs
                if not meme_diversity.has_key(meme):
                    meme_diversity[meme] = {month : {day : {'diversity' : diversity, 'msgs' : total_msgs}}}
                elif not meme_diversity[meme].has_key(month):
                    meme_diversity[meme][month] = {day : {'diversity' : diversity, 'msgs' : total_msgs}}
                elif not meme_diversity[meme][month].has_key(day):          
                    meme_diversity[meme][month][day] = {'diversity' : diversity, 'msgs' : total_msgs}

        meme_diversity[meme]['total_diversity'] = (len(total_different_ids) * 100.0) / total_total_msgs
        meme_diversity[meme]['total_msgs'] = total_total_msgs
        
    print 'Writing file...'   
    json.dump(meme_diversity, open('./' + DATA + '/meme_source_diversity.json', 'w'), indent=2)
    
def _add_meme(meme_days, meme, month, day, user_id, from_id):
    # Adds a meme to the dict
    list_to_add = []
    if from_id != '':
        list_to_add.append(from_id)
        
    if not meme_days.has_key(meme):
        meme_days[meme] = {month : {day : {'total' : 1, user_id : {'total' : 1, 'from_id': list_to_add}}}}
    elif not meme_days[meme].has_key(month):
        meme_days[meme][month] = {day : {'total' : 1, user_id : {'total' : 1, 'from_id': list_to_add}}}
    elif not meme_days[meme][month].has_key(day):          
        meme_days[meme][month][day] = {'total' : 1, user_id : {'total' : 1, 'from_id': list_to_add}}
    elif not meme_days[meme][month][day].has_key(user_id):
        try:
            total = meme_days[meme][month][day]['total']
            total += 1
        except:
            total = 1  
        meme_days[meme][month][day]['total'] = total
        meme_days[meme][month][day][user_id]['total'] = 1
        meme_days[meme][month][day][user_id]['from_id'] = list_to_add

    else:  
        meme_days[meme][month][day]['total'] += 1
        meme_days[meme][month][day][user_id]['total'] += 1
        if from_id != '':
            unique_ids = set(meme_days[meme][month][day][user_id]['from_id'])
            unique_ids.add(from_id)
            meme_days[meme][month][day][user_id]['from_id'] = list(unique_ids)

        
def filter_relevant_memes():
    # filters relevant memes acording to the quantile specified in FILTER_QUANTILE
    print 'Filtering relevant memes'
    print ' - Loading file...' 
    meme_diversity = json.load(open('./' + DATA + '/meme_source_diversity.json', 'r'))
    
    # get the msg number for each meme
#    msg_totals = [meme_diversity[meme]['total_msgs'] for meme in meme_diversity]
    # calculate the minumun msg ammount necessary to be relevant. We do this taking
    # the higher 5%.
#    totals = pd.Series(msg_totals)
#    min_msgs = totals.quantile(FILTER_QUANTILE)
    min_msgs = 39 #days captured X 3
    print 'Minimun messages to be relevant: %s' % (min_msgs)
    # get the relevant memes
    relevant_memes = [meme for meme in meme_diversity if meme_diversity[meme]['total_msgs'] >= min_msgs]
    # copy their data
    filtered_meme_diversity = {}
    for meme in relevant_memes:
        filtered_meme_diversity[meme] = meme_diversity[meme]
    
    print ' - Total relevant memes: %s' % (len(filtered_meme_diversity))
    print ' - Writing file...' 
    json.dump(filtered_meme_diversity, open('./' + DATA + '/meme_source_diversity_filtered.json', 'w'), indent=2)

def build_viral_network(filter_relevant = True):
    print 'Building viral network...'
    meme_count = json.load(open('./' + DATA + '/meme_count.json', 'r'))
    if filter_relevant:
        filtered_meme_diversity = json.load(open('./' + DATA + '/meme_source_diversity_filtered.json', 'r'))
        filtered_memes = [meme for meme in filtered_meme_diversity]
    G = nx.DiGraph()
    for i, meme in enumerate(meme_count):  
        if i % 2000 == 0:
            print 'Processing %s of %s meme' % (i, len(meme_count))
            
        if filter_relevant:
            if not meme in filtered_memes:
                continue
            
        for month in meme_count[meme]:
            for day in meme_count[meme][month]:
                for id_user in meme_count[meme][month][day]:
                    if id_user != 'total':
                        from_ids = meme_count[meme][month][day][id_user]['from_id']
                        for from_id in from_ids:
                            G.add_edge(from_id, id_user, meme = meme)
                            
    print 'Writing file...'
    nx.write_gexf(G, open('./networks/viral_memes.gexf','w'))
    
def build_influence_network():
    print 'Building influence network...'
    url_count = json.load(open('./' + DATA + '/url_count.json', 'r'))
    retweet_count = json.load(open('./' + DATA + '/retweet_count.json', 'r'))
    G = nx.DiGraph()    
    
    for id_from in url_count:
        total_urls = url_count[id_from]
        if total_urls > 3:
            G.add_node(id_from, total_urls=total_urls)
            try:
                for id_to in retweet_count[id_from]:
                    weight = retweet_count[id_from][id_to] * 1.0 / total_urls
                    G.add_edge(id_from, id_to, weight=weight)
            except:
                pass
            
    print 'Writing file...'
    nx.write_gexf(G, open('./networks/influence.gexf','w'))

    
if __name__=='__main__':  
    print 'Starting...'
    
#    count_meme_appearances()
#    count_meme_id_diversity()
#    filter_relevant_memes()
#    build_viral_network()
    build_influence_network()
    
    print 'DONE'