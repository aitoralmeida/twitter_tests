import json
from flask import Flask, request
from pymongo import MongoClient

from config import WEB_CREDENTIALS, MONGODB_PUSHES_URI

# MONGODB_PUSHES_URI = 'mongodb://localhost/twitter_tests'

mongo_client = MongoClient(MONGODB_PUSHES_URI)
db = mongo_client.twitter_tweets

app = Flask(__name__)

def error(msg):
    return json.dumps({ 'error' : True, 'msg' : msg })

@app.route("/", methods = ['GET', 'POST'])
def index():
    if request.method != 'POST':
        return error("You must make a POST request")
    if request.authorization is None:
        return error("You must provide basic authorization credentials")
    key = u':'.join( (request.authorization['username'], request.authorization['password']))
    user = WEB_CREDENTIALS.get(key)
    if user is None:
        return error("Invalid credentials")

    raw_tweets = request.json
    for raw_tweet in raw_tweets:
        raw_tweet[u'origin_twitter_streamer'] = user

    # Bulk insert
    db.raw_tweets.insert(raw_tweets)

    return json.dumps({ 'error' : False, 'msg' : 'OK, %s tweets inserted' % len(raw_tweets) })

if __name__ == '__main__':
    app.run(debug = True)
