import json

print "Loading data..."
user_names_by_name = json.load(open("user_names_by_name.json"))
# { 'limited_user_name' : [ 'user_name1', 'user_name2' ... ]

calculated = json.load(open("calculated.json"))
# { 'limited_user_name' : 'male' or 'female' or 'unknown'

user_names = json.load(open("user_names.json"))
# { 'user_name' : { 'ids' : [ 123, 123, 123], 'tweets' : 359 } }

print "done."

names_by_user_name = {
    # user_name1 : limited_user_name
}

print "Calculating names by user name..."
for name in user_names_by_name:
    for user_name in user_names_by_name[name]:
        names_by_user_name[user_name] = name

print "done"

info_by_gender = {
# gender : {
#      'tweets' : 0
#      'user_ids' : 0
# }
}

for gender in set(calculated.values()):
    info_by_gender[gender] = {
        'tweets' : 0,
        'user_ids' : 0,
        'user_names' : 0,
    }

print "Calculating info by gender..."

total_tweets = 0
total_user_ids = 0
total_user_names = 0
classified_user_names = 0
classified_user_ids = 0
classified_tweets = 0

skipped = 0
for user_name in user_names:
    info = user_names[user_name]
    total_tweets += info['tweets']
    total_user_ids += len(info['ids'])
    total_user_names += 1

    name = names_by_user_name[user_name]
    if name in calculated:
        gender = calculated[name]
    else:
        gender = 'unknown'
        skipped += 1

    if gender in ('male', 'female'):
        classified_user_names += 1
        classified_tweets += info['tweets']
        classified_user_ids += len(info['ids'])

    info_by_gender[gender]['tweets'] += info['tweets']
    info_by_gender[gender]['user_ids'] += len(info['ids'])
    info_by_gender[gender]['user_names'] += 1

print "done."

print
print "Skipped %s" % skipped
print "User names: %s" % total_user_names
print "User ids: %s" % total_user_ids
print "Tweets: %s" % total_tweets
print
print "Classified as male or female: %s user names out of %s (%.2f%%)" % (classified_user_names, total_user_names, 100.0 * classified_user_names / total_user_names)
print "Classified as male or female: %s user ids out of %s (%.2f%%)" % (classified_user_ids, total_user_ids, 100.0 * classified_user_ids / total_user_ids)
print "Classified as male or female: %s tweets out of %s (%.2f%%)" % (classified_tweets, total_tweets, 100.0 * classified_tweets / total_tweets)

json.dump(info_by_gender, open('info_by_gender.json', 'w'), indent = 4)
