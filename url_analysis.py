# -*- coding: utf-8 -*-
"""
Created on Wed Oct 29 16:23:47 2014

@author: aitor
"""

from glob import glob
import json
import gzip
import operator

CORPUS = 'corpus' #corpus, corpus_lite

def count_cited_urls():
    url_count = {}
    
    for i, file_name in enumerate(glob('./' + CORPUS + '/*.txt.gz')):
        print 'Processing %i of %i files' % (i, len(glob('./' + CORPUS + '/*.txt.gz')))
        with gzip.open(file_name, 'r') as f:
            for line in f:
                try:
                    tweet = json.loads(line)
                except:
                    continue
            
                try:
                    urls = [m['expanded_url'] for m in tweet['entities']['urls']]
                    for url in urls:
                        if url_count.has_key(url):
                            url_count[url] += 1
                        else:
                            url_count[url] = 1
                except:
                    continue
                
    return url_count
    
def count_sites(url_count):
    site_count = {}
    for url in url_count:
        url = url.replace('https://', '')
        url = url.replace('http://', '')
        site = url.split('/')[0]
        if site_count.has_key(site):
            site_count[site] += 1
        else:
            site_count[site] = 1
            
    return site_count
        
    
if __name__=='__main__':  
    print 'Starting...'
    url_count = count_cited_urls()
    site_count = count_sites(url_count)
    sorted_sites = sorted(site_count.items(), key=operator.itemgetter(1))
    print sorted_sites
    print 'Done'