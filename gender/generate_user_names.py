import os
import sys
import glob
import json
import gzip

CORPUS_PATH = '../../' # XXX: REMOVE THIS

files = glob.glob(CORPUS_PATH + 'tweets*.txt.gz')

# Get user_names if exists
USER_NAMES = 'user_names.json'
if os.path.exists(USER_NAMES):
    user_names = json.load(open(USER_NAMES))
else:
    user_names = {
         # user_name : {
         #     'ids' : [ 123131, 2131231],
         #     'tweets' : 345
         # }
    }

FILES_PROCESSED = 'files_processed.json'
if os.path.exists(FILES_PROCESSED):
    files_processed = json.load(open(FILES_PROCESSED))
else:
    files_processed = []

for fname in files:
    if fname in files_processed:
        print "%s already processed. Skipping..." % fname
        continue
    size = os.stat(fname).st_size
    mb = size / 1024 / 1024
    print "Processing %s (%s MB)..." % (fname, mb)
    original = len(user_names)
    with gzip.open(fname) as f:
        for line in f:
            try:
                contents = json.loads(line)
            except:
                print >> sys.stderr, "Invalid line: %s" % line
                continue

            user_name = contents['user']['name']
            user_id = contents['user']['id']
            if user_name not in user_names:
                user_names[user_name] = { 'tweets' : 0, 'ids' : [] }
            
            if user_id not in user_names[user_name]['ids']:
                user_names[user_name]['ids'].append(user_id)
            user_names[user_name]['tweets'] = 1

    print "done. %s new names" % (len(user_names) - original)
    user_names_json = open("user_names.json", "w")
    json.dump(user_names, user_names_json, indent = 4)

    files_processed.append(fname)
    json.dump(files_processed, open(FILES_PROCESSED, 'w'), indent = 4)

