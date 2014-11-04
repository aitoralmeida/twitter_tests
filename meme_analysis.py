# -*- coding: utf-8 -*-
"""
Created on Tue Nov 04 09:49:31 2014

We consider both tags and urls as memes

@author: aitor
"""

from glob import glob
import json
import gzip

CORPUS = 'corpus' #corpus, corpus_lite
SEED = ['p2', 'tcot', 'gov', 'dem', 'dems', 'gop']

def count_meme_appearance():    
    meme_days = {} #{'meme' : {'month' : {'day' : 14}}}
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
                    post_datetime = tweet['created_at']  
                    tokens = post_datetime.split(' ')
                    month = tokens[1]
                    day = tokens[2]                                  
                except:
                    continue
                
                try:
                    tags = [x['text'] for x in tweet['entities']['hashtags']]
                    for tag in tags:
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
                
                
                        
    json.dump(meme_days, open('meme_count.json', 'w'), indent=2)
    
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
        meme_days[meme][month][day][day][user_id] = 1
    else:  
        meme_days[meme][month][day]['total'] += 1
        meme_days[meme][month][day][user_id] += 1
    
if __name__=='__main__':  
    print 'Starting...'
    
    count_meme_appearance()
    
    print 'DONE'