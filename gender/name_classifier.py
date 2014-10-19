# -*-*- encoding: utf8 -*-*-
import os
import sys
import re
import time
import json
import pprint
from collections import defaultdict

USE_CACHE = True

NEW_GENDERIZE = 'genders_genderize_new.json'
CALCULATED = 'calculated.json'
GENDERS_GENDERIZE = 'genders_genderize.json'
USER_NAMES = 'user_names.json'
USER_NAMES_BY_NAME = 'user_names_by_name.json'
MANY_USERS_MAX = 10
MANY_USERS_MIN = 3
MINIMUM_NAME_LENGTH = 4

if os.path.exists(GENDERS_GENDERIZE):
    old_genders_genderize = json.load(open(GENDERS_GENDERIZE))
else:
    print >> sys.stderr, "%s not found" % GENDERS_GENDERIZE
    old_genders_genderize = {}
genders_genderize = {}
for key in old_genders_genderize:
    genders_genderize[key.lower()] = old_genders_genderize[key]

if os.path.exists(NEW_GENDERIZE):
    new_genders_genderize = json.load(open(NEW_GENDERIZE))
else:
    print >> sys.stderr, "%s not found" % NEW_GENDERIZE
    new_genders_genderize = {}

if USE_CACHE and os.path.exists(USER_NAMES_BY_NAME):  
    print "Loading %s. Delete to re-create." % USER_NAMES_BY_NAME
    user_names_by_user = json.load(open(USER_NAMES_BY_NAME))
else:
    print "Generating %s..." % USER_NAMES_BY_NAME
    initial = time.time()
    user_names = json.load(open(USER_NAMES))
    print "%s different usernames" % len(user_names)

    camelcase_regex = re.compile(r'^(.*)([a-z]{2})([A-Z0-9])(.*)$')

    user_names_by_user = defaultdict(list)

    for username in user_names:
        user = username
        # Remove weird characters
        user = u''.join( [ c for c in user.strip() if ord(c) < 256 ] )
        # Remove spaces in the beginning / end
        user = user.strip()
        # CarlosMariaFernandez => Carlos MariaFernandez
        user = camelcase_regex.sub(r'\1\2 \3\4', user)
        # Carlos_MariaFernandez => Carlos MariaFernandez
        user = user.replace(u"_", u" ")
        user = user.replace(u"(", u" ")
        user = user.replace(u".", u" ")
        user = user.replace(u"*", u" ")
        user = user.replace(u"'", u" ")
        user = user.replace(u"€", u" ")
        user = user.replace(u"!", u" ")
        user = user.replace(u"®", u" ")
        user = user.replace(u":", u" ")
        user = user.strip()

        # Carlos MariaFernandez => Carlos
        user = user.split(" ")[0].lower()
        # Store
        user_names_by_user[user].append(username)

    valid_names = []
    for username in user_names_by_user:
        if len(username) > MINIMUM_NAME_LENGTH and len(user_names_by_user[username]) > MANY_USERS_MAX:
            valid_names.append(username)

    valid_names.sort(lambda name1, name2 : cmp(len(name1), len(name2)), reverse = True)

    # Find real names by how many elements are found, and iterate a second time to find names like "carlosfernandez"
    suggestions = defaultdict(int)
    for valid_name in valid_names:
        for username in list(user_names_by_user):
            if username.startswith(valid_name) and username != valid_name and len(user_names_by_user[username]) < MANY_USERS_MIN:
                suggestions[len(user_names_by_user[username])] += 1
                if len(user_names_by_user[username]) > 1:
                    # print u"Potential change: %s by %s" % (username, valid_name)
                    users = user_names_by_user.pop(username)
                    user_names_by_user[valid_name].extend(users)

    # pprint.pprint(suggestions)

    json.dump(user_names_by_user, open(USER_NAMES_BY_NAME, 'w'), indent = 4)
    end = time.time()
    print "[done] %.2f seconds" % (end - initial)

print "%s different names" % len(user_names_by_user)

distribution_people = defaultdict(int)
distribution_names = defaultdict(int)
distribution_people_unknown = defaultdict(int)
distribution_names_unknown = defaultdict(int)

calculated_names = {}

THRESHOLD = 5

males   = 0
females = 0
genderize_0 = 0
new_genderize_0 = 0
under_threshold = 0
unknown = 0

total_number = 0

for username in user_names_by_user:
    number_of_names = len(user_names_by_user[username])
    total_number += number_of_names
    if number_of_names > THRESHOLD:
        accumulated_number_of_names = THRESHOLD
    else:
        accumulated_number_of_names = number_of_names
    distribution_people[accumulated_number_of_names] += number_of_names
    distribution_names[accumulated_number_of_names] += 1

    if username in genders_genderize:
        result = genders_genderize[username]
        calculated_names[username] = result
        if result == 'male':
            males += number_of_names
        elif result == 'female':
            females += number_of_names
        else:
            genderize_0 += number_of_names
    elif username in new_genders_genderize:
        result = new_genders_genderize[username]
        if result['gender'] is None:
            new_genderize_0 += 1
            calculated_names[username] = 'unknown'
            unknown += number_of_names
            distribution_people_unknown[accumulated_number_of_names] += number_of_names
            distribution_names_unknown[accumulated_number_of_names] += 1
        elif result['probability'] < 0.5:
            under_threshold += 1
        elif result['gender'] == 'male':
            males += number_of_names
        elif result['gender'] == 'female':
            females += number_of_names
        else:
            print "CAN'T PROCESS THIS", result
    else:
        calculated_names[username] = 'unknown'
        unknown += number_of_names
        distribution_people_unknown[accumulated_number_of_names] += number_of_names
        distribution_names_unknown[accumulated_number_of_names] += 1

json.dump(calculated_names, open(CALCULATED, 'w'))

print " - People distribution among name length:"
pprint.pprint(dict(distribution_people))
print " - Name distribution among name length:"
pprint.pprint(dict(distribution_names))

print
print "Males:", males, '%.2f%%' % (100.0 * males / total_number)
print "Females:", females, '%.2f%%' % (100.0 * females / total_number)
print "Genderize 0:", genderize_0
print "New Genderize 0:", new_genderize_0
print "Under threshold:", under_threshold
print "Unknown:", unknown
print 
print "From which unknown:"
print 
print " - People distribution among name length:"
pprint.pprint(dict(distribution_people_unknown))
print " - Name distribution among name length:"
pprint.pprint(dict(distribution_names_unknown))

