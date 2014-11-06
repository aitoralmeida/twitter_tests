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

CORPUS = 'corpus' #corpus, corpus_lite
DATA = 'data' #data output folder
SEED = ['p2', 'tcot', 'gov', 'dem', 'dems', 'gop']
FILTER_QUANTILE = 0.95 # we only use the 5% most relevant memes

def count_meme_appearances():  
    print 'Counting meme appearances'
    # Counts how many times a meme appears and which are the injection points
    meme_days = {} #{'meme' : {'month' : {'day' : {'id' : 14, 'id2' : 1}}}
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
                
                try:
                    tags = [x['text'] for x in tweet['entities']['hashtags']]
                    for t in tags:
                        tag = t.lower()
                        if not tag in SEED:                                             
                            _add_meme(meme_days, tag, month, day, user_id)                    
                except:
                    pass
                
                try:
                    urls = [m['expanded_url'] for m in tweet['entities']['urls']]
                    for url in urls:
                        _add_meme(meme_days, url, month, day, user_id)   
                except:
                    pass
    print 'Writing file...'                    
    json.dump(meme_days, open('./' + DATA + '/meme_count.json', 'w'), indent=2)
    
def count_meme_id_diversity():
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
                        total_msgs += meme_count[meme][month][day][id_user] 
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
    
def _add_meme(meme_days, meme, month, day, user_id):       
    if not meme_days.has_key(meme):
        meme_days[meme] = {month : {day : {'total' : 1, user_id : 1}}}
    elif not meme_days[meme].has_key(month):
        meme_days[meme][month] = {day : {'total' : 1, user_id : 1}}
    elif not meme_days[meme][month].has_key(day):          
        meme_days[meme][month][day] = {'total' : 1, user_id : 1}
    elif not meme_days[meme][month][day].has_key(user_id):
        try:
            total = meme_days[meme][month][day]['total']
            total += 1
        except:
            total = 1  
        meme_days[meme][month][day]['total'] = total
        meme_days[meme][month][day][user_id] = 1

    else:  
        meme_days[meme][month][day]['total'] += 1
        meme_days[meme][month][day][user_id] += 1
        
def filter_relevant_memes():
    print 'Filtering relevant memes'
    print ' - Loading file...' 
    meme_diversity = json.load(open('./' + DATA + '/meme_source_diversity.json', 'r'))
    msg_totals = [meme_diversity[meme]['total_msgs'] for meme in meme_diversity]
    
    totals = pd.Series(msg_totals)
    min_msgs = totals.quantile(FILTER_QUANTILE) # only use the 5% most relevant
    relevant_memes = [meme for meme in meme_diversity if meme_diversity[meme]['total_msgs'] >= min_msgs]
    filtered_meme_diversity = {}
    for meme in relevant_memes:
        filtered_meme_diversity[meme] = meme_diversity[meme]
    
    print ' - Total filtered memes: %s' % (len(filtered_meme_diversity))
    print ' - Writing file...' 
    json.dump(filtered_meme_diversity, open('./' + DATA + '/meme_source_diversity_filtered.json', 'w'), indent=2)
    

    
if __name__=='__main__':  
    print 'Starting...'
    
    count_meme_appearances()
    count_meme_id_diversity()
    filter_relevant_memes()
    
    print 'DONE'