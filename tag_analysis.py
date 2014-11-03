# -*- coding: utf-8 -*-
"""
Created on Fri Oct 17 09:08:11 2014

@author: aitor
"""
from glob import glob
import json
import gzip

CORPUS = 'corpus' #corpus, corpus_lite

republican = [] #list of republican account ids
democrat = [] #list of democrat account ids
SEED = ['p2', 'tcot', 'gov', 'dem', 'dems', 'gop']
MIN_JACCARD = 0.005

def select_related_tags_jaccard(seed_tags):
# To identify which hastags are related with some event we are going to use some hastags 
# as seeds to study the co-ocurrence. To do this we are going to analyze which
# hashtags co-occur in at least one tweet and we are going to ranked them using 
# the Jaccard Coefficient. The similarity threshold used in other papers is 0.005
#
# Coef = |S disjuction T| / |S union T|   => 
# Coef = Number of tweets with a seed and the hashtag / number of tweets with a seed + number of tweets with the hashtag
# Where S is the tweets that the tag appears with some of the seeds and
# T is the number of tweets that appears without them
    
    tag_count = {} # {'tag' : {'with_seed' : 2, 'tag': 10}}
    total_tweets_seed = 0.0
    
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
            try:
                print '  - ', tag, tweets_with_seed_coocurrence, jaccard_coef
            except:
                pass
        if jaccard_coef > MIN_JACCARD:
            results.append(tag)        
    
    return results
    
def _count_side_tags():
# Count how many times each relevant tag has been used by each side. It will
# be used to calculate the political valence of the tags. 
    tag_count = {} # {'tag' : {'democrat' : 2, 'republican': 10}}
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
                    tags = [x['text'] for x in tweet['entities']['hashtags']]
                except:
                    continue
                
                if user_id in democrat:
                    for t in tags:
                        if tag_count.has_key(t):
                            tag_count[t]['democrat'] += 1
                        else:
                            tag_count[t] = {'democrat' : 1, 'republican' : 0}
                elif user_id in republican:
                    for t in tags:
                        if tag_count.has_key(t):
                            tag_count[t]['republican'] += 1
                        else:
                            tag_count[t] = {'democrat' : 0, 'republican' : 1}
                            
    return tag_count
    
def calculate_political_valence(tag_count):
# Uses the formula in "Political Polarization on Twitter"
# Calculates the political valence of each tag depending on its use by each side.
# Takes values between 1 (democrat) and -1 (republican)
    total_democrat = 0
    total_republican= 0
    political_valence = {} # {'tag' : 0.45}
    
    for t in tag_count:
        total_democrat += tag_count[t]['democrat']
        total_republican += tag_count[t]['republican']
               
    for t in tag_count:
        tag_democrat = tag_count[t]['democrat'] * 1.0
        tag_republican = tag_count[t]['republican'] * 1.0
        
        political_valence[t] = 2.0 * ( (tag_democrat/total_democrat)/((tag_republican/total_republican) + (tag_democrat/total_democrat)) ) - 1
        
    return political_valence
    
def calculate_user_valence_by_tag(tags_valence):
# Calculates the political valence of an account based on the tags that it uses.
# Takes values between 1 (democrat) and -1 (republican)
    users_valence = {}
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
                    tags = [x['text'] for x in tweet['entities']['hashtags']]
                except:
                    continue


                if len(tags) > 0:
                    temp_valence = 0
                    political_tags = 0
                    
                    for tag in tags:
                        if tags_valence.has_key(tag):
                            temp_valence += tags_valence[tag]
                            political_tags += 1
                    
                    if political_tags > 0: # valence is only calculated if the tweet has any politically relevant tag                   
                        temp_valence = temp_valence * 1.0 / political_tags    
    
                        if users_valence.has_key(user_id):
                            users_valence[user_id] = (users_valence[user_id] + temp_valence) / 2.0
                        else:
                            users_valence[user_id] = temp_valence
                    
    return users_valence
    
if __name__=='__main__':  
    print 'Starting...'
    
    # identify the related tags
    results = select_related_tags_jaccard(SEED)
    print results
    
#    # calculate tags political valence
#    tag_count = _count_side_tags()
#    political_valence = calculate_political_valence(tag_count)
#    print political_valence
#    # calculate account valences
#    user_valences = calculate_user_valence_by_tag(political_valence)
#    print user_valences
    
    print 'DONE'