# -*- coding: utf-8 -*-
"""
Created on Mon Nov 03 12:33:34 2014

Based on http://www.csc.ncsu.edu/faculty/healey/tweet_viz/

@author: aitor
"""

import scipy.stats

class InsuficientDataError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

anew_words = {}


def _load_anew():
    #TODO implement it
    return {}
    
def get_tweet_values(text):
    valence_acum = 0
    valence_weights = 0
    arousal_weights = 0
    arousal_acum = 0    
    
    total_anew_words = 0    
    words = text.split(' ')
    for w in words:
        if w in anew_words:
            total_anew_words += 1
            
            # The mean is not calculated arithmetically, isntead is done as
            # explained in http://www.csc.ncsu.edu/faculty/healey/tweet_viz/. We
            # use the probability density function of a normal distribution to
            # estimate the probability of the word's rating falling exactly at
            # the mean. The probabilities are applied as weights when we sum the
            # means.
            valence_mean = anew_words[w]['valence_mean']
            valence_std = anew_words[w]['valence_std']
            valence_weight = scipy.stats.norm(valence_mean, valence_std).pdf(valence_mean)
            valence_weights += valence_weight
            valence_acum += valence_mean * valence_weight
            
            arousal_mean = anew_words[w]['arousal_mean']
            arousal_std = anew_words[w]['arousal_std']
            aroursal_weight = scipy.stats.norm(arousal_mean, arousal_std).pdf(arousal_mean)
            arousal_weights += aroursal_weight
            arousal_acum += arousal_mean * aroursal_weight
            
    if total_anew_words < 2:
        raise InsuficientDataError("Less than 2 ANEW words")
        
    valence = valence_acum / valence_weights
    arousal = arousal_acum / arousal_weights            
        
    return valence, arousal
            
            
        
    
