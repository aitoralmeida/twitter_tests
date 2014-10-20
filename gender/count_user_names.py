import json

user_names = json.load(open("user_names.json"))
print "%s different user names" % len(user_names)

user_ids = 0
tweets = 0
for pack in user_names.values():
    user_ids += len(pack['ids'])
    tweets += pack['tweets']

print "Representing %s Twitter ids and %s tweets" % (user_ids, tweets)
