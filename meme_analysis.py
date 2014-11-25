# -*- coding: utf-8 -*-
"""
Created on Tue Nov 04 09:49:31 2014

We consider both tags and urls as memes

@author: aitor
"""

from glob import glob
import json
import gzip
import networkx as nx
from sentiment_analysis_textblob import get_tweet_polarity
import time

CORPUS = 'corpus' #corpus, corpus_lite
DATA = 'data' #data output folder
SEED = ['p2', 'tcot', 'gov', 'dem', 'dems', 'gop']
FILTER_QUANTILE = 0.99 # we only use the 1% most relevant memes
PROCESSED_INTERVAL = 3600 # 1 hour

def count_meme_appearances(): 
    # Counts how many times a meme appears and which are the injection points
    print 'Counting meme appearances'    
    total_tags = 0
    total_urls = 0
    meme_days = {} #{'meme' : {'month' : {'day' : {'id' : 14, 'id2' : 1}}}
    meme_intervals = {} # {'meme': {'epoch' : {'tweet' : 12, 'retweet' : 23}}}
    epoch_acum = {} # {'epoch' : 12}
    url_count = {} # {'id': 24}
    retweet_count = {} # only those retweets with URLs {id_from : {id_to : 13}}
    start_epoch = 0
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
                
                epoch = time.mktime(time.strptime(post_datetime,"%a %b %d %H:%M:%S +0000 %Y"))
                if i == 0:
                    start_epoch = epoch
                
                if (epoch - start_epoch) >= PROCESSED_INTERVAL:
                    start_epoch = epoch
                
                is_retweet = False            
                from_id = ''
                if tweet.has_key('retweeted_status'):
                    is_retweet = True
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
                            _add_meme_days(meme_days, tag, month, day, user_id, from_id)
                            total_tags += 1
                            _add_meme_interval(meme_intervals, tag, start_epoch, is_retweet)
                            
                            if epoch_acum.has_key(start_epoch):
                                epoch_acum[start_epoch] += 1
                            else:
                                epoch_acum[start_epoch] = 1
                                
                            
                            
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
                        _add_meme_days(meme_days, url, month, day, user_id, from_id) 
                        total_urls += 1
                        _add_meme_interval(meme_intervals, url, start_epoch, is_retweet)
                        if epoch_acum.has_key(start_epoch):
                            epoch_acum[start_epoch] += 1
                        else:
                            epoch_acum[start_epoch] = 1
                except:
                    pass
                
    print 'Total tags: %s' % (total_tags)
    print 'Total URLs: %s' % (total_urls)      
    print 'Writing file...'                    
    json.dump(meme_days, open('./' + DATA + '/meme_count.json', 'w'), indent=2)
    json.dump(meme_intervals, open('./' + DATA + '/meme_intervals.json', 'w'), indent=2)
    json.dump(epoch_acum, open('./' + DATA + '/intervals_accum.json', 'w'), indent=2)
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
    
def _add_meme_days(meme_days, meme, month, day, user_id, from_id):
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
            
def _add_meme_interval(meme_intervals, meme, start_epoch, is_retweet):
    if meme_intervals.has_key(meme):
        if meme_intervals[meme].has_key(start_epoch):
            if is_retweet:
                meme_intervals[meme][start_epoch]['retweet'] += 1
            else:
                meme_intervals[meme][start_epoch]['tweet'] += 1
        else:
            if is_retweet:
                meme_intervals[meme][start_epoch] = {'tweet' : 0, 'retweet' : 1}
            else:
                meme_intervals[meme][start_epoch] = {'tweet' : 1, 'retweet' : 0}                                
    else:
        if is_retweet:
            meme_intervals[meme] = {start_epoch : {'tweet' : 0, 'retweet' : 1}}
        else:
            meme_intervals[meme] = {start_epoch : {'tweet' : 1, 'retweet' : 0}}

        
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
    min_msgs = 21 * 3 #days captured X 3
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
    
def build_influence_network(min_urls = 10):
    print 'Building influence network...'
    url_count = json.load(open('./' + DATA + '/url_count.json', 'r'))
    retweet_count = json.load(open('./' + DATA + '/retweet_count.json', 'r'))
    G = nx.DiGraph()    
    
    for n in url_count:
        G.add_node(n, total_urls = url_count[n], passivity = 1, influence = 1)        
    
    for id_from in url_count:
        total_urls = url_count[id_from]
        try:
            for id_to in retweet_count[id_from]:
                weight = retweet_count[id_from][id_to] * 1.0 / total_urls
                G.add_edge(id_from, id_to, weight=weight)
        except:
            pass
    
    print 'Prunning non relevant nodes...'        
    for node in G.nodes(data = True):
        try:
            total_urls = node[1]['total_urls']
        except:
            total_urls = 0
        # prunning those nodes that have not tweeted enough URLs
        if total_urls < min_urls:
            G.remove_node(node[0]) 
    
    # prunning nodes without connections      
    degrees = G.degree()
    for n in degrees:
        if degrees[n] == 0:
           G.remove_node(n) 
               
        
            
    print 'Writing file...'
    nx.write_gexf(G, open('./networks/influence_network.gexf','w'))
    
def calculate_influence_passivity(m = 30):
    print 'Calculating influence and passivity...'
    G = nx.read_gexf('./networks/influence_network.gexf')
    for i  in range(m):
        print '%s of %s' % (i,m)
        _update_passivity_influence(G)
    
    print 'Writing file...'    
    nx.write_gexf(G, open('./networks/influence_passivity.gexf','w'))
    
def _update_passivity_influence(G):
    #get the data necessary to calculate acceptance rate
    # how many urls id_to has accepted from id_from, how how many urls in total
    acceptance_rates_aux = {} #{id_to : {id_from : 13, 'total: 25}} 
    for edge in G.edges(data=True):
        id_from = edge[0]
        id_to = edge[1]
        weight = edge[2]['weight']
            
        if not acceptance_rates_aux.has_key(id_to):
            acceptance_rates_aux[id_to] = {id_from : weight, 'total' : weight}
        elif not acceptance_rates_aux[id_to].has_key(id_from):
            acceptance_rates_aux[id_to][id_from] = weight
            acceptance_rates_aux[id_to]['total'] += weight
        else:
            print 'ERROR'
              
            
    # calculate acceptance rates        
    acceptance_rates = {} #{id_from : {id_to : 0.1}
    for id_to in acceptance_rates_aux:
        total = acceptance_rates_aux[id_to]['total']
        for id_from in acceptance_rates_aux[id_to]:
            accepted = acceptance_rates_aux[id_to][id_from]
            rate = accepted * 1.0 / total
            if not acceptance_rates.has_key(id_from):
                acceptance_rates[id_from] = {id_to : rate}
            elif not acceptance_rates[id_from].has_key(id_to):
                acceptance_rates[id_from][id_to] = rate
                          
    # calculate the rejection rates
    rejection_rates = {} #{id_from : {id_to : 0.1}
    for id_to in acceptance_rates_aux:
        
        rejected = {}
        for id_from in acceptance_rates_aux[id_to]:
            rejected[id_from] = 1 - acceptance_rates_aux[id_to][id_from]
            total += 1 - acceptance_rates_aux[id_to][id_from]
            
        for id_from in rejected:
            try:
                rate = rejected[id_from] / total
            except:
                continue
            if not rejection_rates.has_key(id_from):
                rejection_rates[id_from] = {id_to : rate}
            elif not rejection_rates[id_from].has_key(id_to):
                rejection_rates[id_from][id_to] = rate
 
    passivities = {}      
    # calculate passivity            
    for edge in G.edges():
        id_from = edge[0]
        id_to = edge[1]     
        rejection = 0
        try:
            rejection = rejection_rates[id_to][id_from]
        except:
            rejection = 1 # there is no edge(id_to, id_from)
        if passivities.has_key(id_from):
            passivities[id_from] += rejection * G.node[id_to]['influence']
        else: 
            passivities[id_from] = rejection * G.node[id_to]['influence']
        
    # update passivities    
    total_passivity = 0
    for id_from in passivities:
        G.node[id_from]['passivity'] = passivities[id_from]
        total_passivity += passivities[id_from]
    
    # calculate influence    
    influences = {}   
    for edge in G.edges():
        id_from = edge[0]
        id_to = edge[1]  
        aceptance = 0
        try:
            aceptance = acceptance_rates[id_from][id_to]
        except:
            aceptance = 0 # there is no edge(id_from, id_to)
        if influences.has_key(id_from):
            influences[id_from] += aceptance * G.node[id_to]['passivity']
        else:
            influences[id_from] = aceptance * G.node[id_to]['passivity']
    
    # update influences    
    total_influences = 0
    for id_from in influences:
        G.node[id_from]['influence'] = influences[id_from]
        total_influences += influences[id_from]
        
    #normalize passivities and influences
    for n in G.nodes(data = True):
        current_influence = n[1]['influence']
        normalized_influence = (current_influence * 1.0) / total_influences
        G.node[n[0]]['influence'] =  normalized_influence
        
        current_passivity = n[1]['passivity']
        normalized_passivity = (current_passivity * 1.0) / total_passivity
        G.node[n[0]]['passivity'] = normalized_passivity 
        
def get_meme_polarity():
    print 'Getting polarities...'
    polarities = {} #{'meme' : {'total' : 5, 'count' : 3, 'avg' : 5/3}}
    polarization = {} #{'meme' : {'positive' : [ids], 'negative' : [ids]}}
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
                except:
                    continue
                
                text = tweet['text']
                polarity = get_tweet_polarity(text)
                
                if polarity != 0:     
                    memes = []
                    try:
                        tags = [x['text'] for x in tweet['entities']['hashtags']]
                        memes += tags
                    except:
                        pass
                    
                    try:
                        urls = [m['expanded_url'] for m in tweet['entities']['urls']]
                        memes += urls
                    except:
                        pass
                    
                    for meme in memes:
                        # meme polarity
                        if polarities.has_key(meme):
                            polarities[meme]['total'] += polarity
                            polarities[meme]['count'] += 1
                            polarities[meme]['avg'] = polarities[meme]['total'] * 1.0 / polarities[meme]['count']
                        else:
                            polarities[meme] = {'total' : polarity, 'count' : 1, 'avg' : polarity}
                        
                        # meme user polarizations
                        if polarity > 0:
                            if polarization.has_key(meme):
                                polarization[meme]['positive'].append(user_id)
                            else:
                                polarization[meme] = {'positive' : [user_id], 'negative' : []}
                        else:
                            if polarization.has_key(meme):
                                polarization[meme]['negative'].append(user_id)
                            else:
                                polarization[meme] = {'positive' : [], 'negative' : [user_id]}
                            
    print 'Writing file...'                    
    json.dump(polarities, open('./' + DATA + '/meme_polarity.json', 'w'), indent=2)      
    json.dump(polarization, open('./' + DATA + '/meme_user_polarization.json', 'w'), indent=2)      
    
def calculate_burstiness():
    print 'Identifiying breaking memes...'
    # {'meme': {'epoch' : {'tweet' : 12, 'retweet' : 23}}}
    meme_intervals = json.load(open('./' + DATA + '/meme_intervals.json', 'r'))
    # {'epoch' : 12}
    epoch_acum = json.load(open('./' + DATA + '/intervals_accum.json', 'r')) 
    total_memes = len(meme_intervals)

    meme_burstiness = {} # {'meme' : { 'epoch#' : 0.54}}
    for j, meme in enumerate(meme_intervals):        
        if j % 1000 == 0:
            print 'Processing %i of %i memes' % (j, total_memes)   
        values = meme_intervals[meme]
        for i, interval in enumerate(values):
            if i == 0:
                continue
            meme_ocurrences_interval = values[interval]['tweet'] + values[interval]['retweet']
            total_ocurrences_interval = epoch_acum[interval]
            meme_ocurrences_until = sum([values[inter]['tweet'] + values[inter]['retweet'] 
                                                    for inter in values if inter <= interval])
            total_ocurrences_until = [epoch_acum[epoch] for epoch in epoch_acum if epoch <= interval]
            
            burstiness = (meme_ocurrences_interval / total_ocurrences_interval) / (meme_ocurrences_until / total_ocurrences_until)
            if meme_burstiness.has_key(meme):
                meme_burstiness[meme][interval] = burstiness
            else:
                meme_burstiness[meme] = {interval : burstiness}

    print 'Writing file...'  
    json.dump(meme_burstiness, open('./' + DATA + '/meme_burstiness.json', 'w'), indent=2)                           
                    
                    
            
    

    
if __name__=='__main__':  
    print 'Starting...'
    
    count_meme_appearances()
#    count_meme_id_diversity()
#    filter_relevant_memes()
#    build_viral_network()
#    build_influence_network()
#    calculate_influence_passivity()
#    get_meme_polarity()
#    calculate_burstiness()
    
    print 'DONE'