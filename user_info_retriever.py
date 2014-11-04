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