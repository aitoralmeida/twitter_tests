# -*- coding: utf-8 -*-
"""
Created on Tue Nov 18 12:46:00 2014

@author: aitor
"""

from textblob import TextBlob

def get_tweet_polarity(tweet_text):
    blob = TextBlob(tweet_text)
    total_polarity = 0
    total_sentences = 0
    for sentence in blob.sentences:
        total_polarity += sentence.sentiment.polarity
        total_sentences += 1
        
    avg_polarity = total_polarity / total_sentences
    
    return avg_polarity