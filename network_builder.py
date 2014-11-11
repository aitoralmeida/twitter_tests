# -*- coding: utf-8 -*-
"""
@author: aitor
"""

import json
from glob import glob
import networkx as nx
import gzip

CORPUS = 'corpus' #corpus, corpus_lite

def build_networks(process_retweets = True):
# Builds the tweets, mentions and total networks.
    G_tot = nx.DiGraph()
    G_retweet = nx.DiGraph()
    G_mentions = nx.DiGraph()
    for i, file_name in enumerate(glob('./' + CORPUS + '/*.txt.gz')):
        if i % 10 == 0:
            print 'Processing %i of %i files' % (i, len(glob('./' + CORPUS + '/*.txt.gz')))
        G_tot, G_retweet, G_mentions = _process_file(file_name, G_tot, G_retweet, G_mentions, process_retweets)
    
    return G_tot, G_retweet, G_mentions

def _process_file(file_name, G_tot, G_retweet, G_mentions, process_retweets = True):
# Process each file
     with gzip.open(file_name, 'r') as f:
        for line in f:
            try:
                tweet = json.loads(line)
            except:
                print 'Badly formed json: ', file_name, line
                continue
            
            if not process_retweets:   
                if tweet.has_key('retweeted_status'):
                    # Tweet is a retweet, skip it
                    continue
                
            try:
                user_id = tweet['user']['id_str']
                user_name = tweet['user']['screen_name']
            except:
                #is a delete
                continue
            
            try:
                if not tweet.has_key('retweeted_status'): 
                    # Retweet mentions are not taken into account, as in 
                    # Meassuring user influence in twitter:the Million followers 
                    # fallacy
                    mentions = [[mention['id_str'],  mention['screen_name']] for mention in tweet['entities']['user_mentions']] 
                    for m_id in mentions:                    
                        G_tot = _add_edge(G_tot, user_id, user_name, m_id[0], m_id[1])       
                        G_mentions = _add_edge(G_mentions, user_id, user_name, m_id[0], m_id[1])    
            except:
                pass
            
            try:
                if tweet.has_key('retweeted_status'):     
                    retweeted_id = tweet['retweeted_status']['user']['id_str']
                    retweeted_label = tweet['retweeted_status']['user']['screen_name']
                    G_tot = _add_edge(G_tot, user_id, user_name, retweeted_id, retweeted_label)  
                    G_retweet = _add_edge(G_retweet, user_id, user_name, retweeted_id, retweeted_label)  
            except:
                pass
            
            if tweet['in_reply_to_user_id'] and tweet['in_reply_to_screen_name']:   
                G_tot = _add_edge(G_tot, user_id, user_name, tweet['in_reply_to_user_id'], tweet['in_reply_to_screen_name']) 
            
            
     return G_tot, G_retweet, G_mentions

def _add_edge(G, a, a_label, b, b_label):
# Add an edge to the node, increments its weight if already exists
    if not G.has_node(a):
        G.add_node(a, label =  a_label)
        
        
    if not G.has_node(b):
        G.add_node(b, label =  b_label) 
        
    if G.has_edge(a, b):
        new_weight = G.edge[a][b]['weight'] + 1
        G.add_edge(a, b, weight = new_weight)
    else:
        G.add_edge(a, b, weight = 1)
    
    return G
    
def _clean_non_representative_edges(G):
# removes non-representative edges, i.e. those that do not have a minimum weight
    MIN_WEIGHT = 4 # minimun weight to consider it representative
    for edge in G.edges(data=True):
        if edge[2]['weight'] <= MIN_WEIGHT:
            G.remove_edge(edge[0], edge[1])
    return G

def _clean_zero_edge_nodes(G):
# remove those nodes that do not have any connections
    degrees = G.degree()
    for node in degrees:
        if degrees[node] == 0:
            G.remove_node(node)
    return G   

def clean_network(path):
# Cleans the network removing edges and nodes
    print 'Loading...'
    G = nx.read_gexf(path)    

    print 'Cleaning...'
    G = _clean_non_representative_edges(G)
    G = _clean_zero_edge_nodes(G)

    print 'Writing...'
    nx.write_gexf(G, open(path.replace('.gexf', '_processed.gexf'),'w'))
    
def colorify_political_valence(path):
# Color nodes according to their political valence. Uses the valence calculated
# in tag_analysis
    print 'Loading...'
    G = nx.read_gexf(path.replace('.gexf', '_processed.gexf'))
    users_valence = json.load(open('users_valence.json', 'r'))
    
    print 'Coloring...'
    for node in G.nodes():
        try:
            valence = users_valence[node]
            if valence > 0:
                valence = 1
            elif valence < 0:
                valence = -1
            else:
                valence = 0
        except:
            valence = 1000 # does not use any political tag
            
        G.node[node]['valence'] = valence

    print 'Writing...'
    nx.write_gexf(G, open(path.replace('.gexf', '_processed.gexf'),'w'))
    
def clean_non_positioned(path):
# Removes those nodes that do not have any political valence
    print 'Loading...'
    G = nx.read_gexf(path.replace('.gexf', '_processed.gexf'))
    
    print 'Cleaning non-positioned...'
    for node in G.nodes():
        if G.node[node]['valence'] == 1000:
            G.remove_node(node)
            
    print 'Writing...'
    nx.write_gexf(G, open(path.replace('.gexf', '_processed.gexf'),'w'))
        

    
    
if __name__=='__main__':     
    print 'Building networks...'
    G_tot, G_retweet, G_mentions = build_networks()
    print 'Writing files...'
    nx.write_gexf(G_tot, open('./networks/total.gexf','w'))
    nx.write_gexf(G_retweet, open('./networks/retweets.gexf','w'))
    nx.write_gexf(G_mentions, open('./networks/mentions.gexf','w'))
    
    networks = ['./networks/total.gexf', './networks/retweets.gexf','./networks/mentions.gexf']
    for n in networks:
        print 'Processing:', n
        clean_network(n)
#        colorify_political_valence(n)
#        clean_non_positioned(n)


    print 'DONE'

