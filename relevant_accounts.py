# -*- coding: utf-8 -*-
"""
Created on Thu Dec 04 16:30:40 2014

@author: aitor
"""

import user_info_retriever

lists_congress = {'cspan' : 'members-of-congress', 'verified': 'us-congress'}
lists_representatives = {'gov' : 'us-house', 'cspan' : 'u-s-representatives'}
lists_senate = {'gov' : 'us-senate', 'cspan' : 'senators'}
lists_goverment = {'gov' : 'us-cabinet'}
lists_media = {'cspan' : 'congressional-media'}

def _get_lists_members(lists):
    user_list = []
    for l in lists:
        user_list += user_info_retriever.get_list_members(l, lists_congress[l])
    
    members = {}    
    for user in set(user_list):
        members[user[0]] = user[1]
        
    return members
    
def _get_congress_members():
    return _get_lists_members(lists_congress)
    
def _get_house_members():
    return _get_lists_members(lists_representatives)
    
def _get_senate_members():
    return _get_lists_members(lists_senate)

def _get_government_members():
    return _get_lists_members(lists_goverment)

def _get_media_members():
    return _get_lists_members(lists_media)



        
        