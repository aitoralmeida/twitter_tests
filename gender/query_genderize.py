import os
import sys
import genderize
import json

ALREADY_CHECKED = 'genders_genderize_new.json'
ALREADY_CHECKED_BACKUP = 'genders_genderize_new.json.bak'

if os.path.exists(ALREADY_CHECKED):
    already_checked = json.load(open(ALREADY_CHECKED))
else:
    already_checked = {
        # name : {
        #     genderize response
        # }
    }

if os.path.exists('genders_genderize.json'):
    existing_genders = json.load(open("genders_genderize.json"))
else:
    print >> sys.stderr, "%s not found. Starting from scratch." % 'genders_genderize.json'
    existing_genders = {}

USER_NAMES_BY_NAME = 'user_names_by_name.json'
if os.path.exists(USER_NAMES_BY_NAME):
    usernames = json.load(open(USER_NAMES_BY_NAME))
else:
    print >> sys.stderr, "%s not found." % USER_NAMES_BY_NAME
    sys.exit(-1)

names_and_frequency = [ 
    # (name, number)
]

for username in usernames:
    if username in existing_genders:
        continue

    number_of_usernames = len(usernames[username])
    if number_of_usernames == 1:
        continue

    names_and_frequency.append( (username, number_of_usernames) )

names_and_frequency.sort( lambda (n1, f1), (n2, f2) : cmp(f1, f2), reverse = True)

g = genderize.Genderize()

BUFFER_SIZE = 50
the_buffer = []

missing_names = [ name 
    for name, frequency in names_and_frequency
    if name not in already_checked
]

print "Missing names: %s" % len(missing_names)

processed = 0

def process_buffer(the_buffer):
    results = g.get(the_buffer)
    for result in results:
        already_checked[result['name']] = result

    json.dump(already_checked, open(ALREADY_CHECKED, 'w'))
    json.dump(already_checked, open(ALREADY_CHECKED_BACKUP, 'w'))

for name in missing_names:
    the_buffer.append(name)
    if len(the_buffer) == BUFFER_SIZE:
        process_buffer(the_buffer)
        processed += BUFFER_SIZE
        print "Processed: %s of %s" % (processed, len(missing_names))
        the_buffer = []

if the_buffer:
    process_buffer(the_buffer)
    print "Processed: %s of %s" % (len(missing_names), len(missing_names))

print "Finished"
