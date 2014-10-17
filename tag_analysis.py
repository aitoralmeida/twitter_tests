# -*- coding: utf-8 -*-
"""
Created on Fri Oct 17 09:08:11 2014

@author: aitor
"""
from glob import glob
import json

CORPUS = 'corpus' #corpus, corpus_lite

def select_related_tags_jaccard(seed_tags):
    # To identify which hastags are related with some event we are going to use some hastags 
    # as seeds to study the co-ocurrence. To do this we are going to analyze which
    # hashtags co-occur in at least one tweet and we are going to ranked them using 
    # the Jaccard Coefficient. The similarity threshold used in other papers is 0.005
  
    # Coef = |S disjuction T| / |S union T|   => 
    # Coef = Number of tweets with a seed and the hashtag / number of tweets with a seed + number of tweets with the hashtag
    # Where S is the tweets that the tag appears with some of the seeds and
    # T is the number of tweets that appears without them
    
    tag_count = {} # {'tag' : {'with_seed' : 2, 'tag': 10}}
    total_tweets_seed = 0.0
    
    for i, file_name in enumerate(glob('./' + CORPUS + '/*.txt')):
        print 'Processing %i of %i files' % (i, len(glob('./' + CORPUS + '/*.txt')))
        with open(file_name, 'r') as f:
            for line in f:
                try:
                    tweet = json.loads(line)
                except:
                    continue
            
                try:
                    user_id = tweet['user']['id']
                    tags = [x['text'] for x in tweet['entities']['hashtags']]
                except:
                    continue
                
                with_seed = False                
                for seed in seed_tags:
                    if seed in tags:
                        with_seed = True
                        total_tweets_seed += 1
                        break
                        
                    
                for tag in tags:
                    if with_seed:
                        if tag_count.has_key(tag):
                            tag_count[tag]['with_seed'] += 1
                        else:
                            tag_count[tag] = {'with_seed' : 1, 'tag' : 1}
                    else:
                        if tag_count.has_key(tag):
                            tag_count[tag]['tag'] += 1
                        else:
                            tag_count[tag] = {'with_seed' : 0, 'tag' : 1}
                            
    results = []                       
    for tag in tag_count:
        tweets_with_seed_coocurrence = tag_count[tag]['with_seed'] * 1.0
        tweets_tag_total = tag_count[tag]['tag'] * 1.0
        jaccard_coef = tweets_with_seed_coocurrence / (total_tweets_seed + tweets_tag_total)
        if tweets_with_seed_coocurrence > 0:
            print '  - ', tag, tweets_with_seed_coocurrence, jaccard_coef
        if jaccard_coef > 0.005:
            results.append(tag)        
    
    return results
    
if __name__=='__main__':  
    print 'Starting...'
    results = select_related_tags_jaccard(['p2','tcot'])
    print results
    print 'DONE'