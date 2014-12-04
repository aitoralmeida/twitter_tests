# -*- coding: utf-8 -*-
"""
Created on Tue Nov 04 11:09:59 2014

@author: aitor
"""

import tweepy

CONSUMER_KEY = '7WVJjUFxjG07hT1boGuS167gJ'
CONSUMER_SECRET ='faY2XRyV4zUO1ScI38T6PsXcVLutOzFgMcAi0VOabgvkq2fXtl'
OAUTH_TOKEN = '2833997469-weiExfPXQ3Mscii21BD5kZHb1srghWzuuLaYLHa'
OAUTH_TOKEN_SECRET = 'B4KYovQBMjnziTeSi1FrkYFOqeKsr4cm76aIrjoknXZUn'


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

api = tweepy.API(auth)

def get_id_from_screen_name(screen_name):
    user = api.get_user(screen_name)
    return user.id_str
    
def get_screen_name_from_id(id_str):
    user = api.get_user(id_str)
    return user.screen_name
    
def get_name(user):
    name = api.get_user(user).name
    return name

def get_followers(id_str):
    user = api.get_user(id_str)
    followers = user.followers_ids()
    return followers
    
def get_friends(id_str):
    user = api.get_user(id_str)
    friends = user.friends()
    friend_ids = []
    for f in friends:
        friend_ids.append(f.id_str)
    return friend_ids
    
def get_relations(id_str):
    relations = {} #{'friends' : [], 'followers': []}
    user = api.get_user(id_str)
    followers = user.followers_ids()
    friends = user.friends()
    relations['friends'] = [f.id_str for f in friends]
    relations['followers'] = followers
    
    return relations
    
def update_relations(id_list):
    all_relations = {} # {'123456789' : {'friends' : [], 'followers': []}}
    for user_id in id_list:
        relations = get_relations(user_id)
        all_relations[user_id] = relations
    
    return all_relations
    
def get_list_members(username, listname):
    members_data = tweepy.Cursor(api.list_members, username, username).items()
    member_accounts = [(u.screen_name, u.name) for u in members_data]
    return member_accounts
    
    
print get_screen_name_from_id("91734908")
    