from datetime import datetime
import sys
import os
from collections import defaultdict

import sqlite3
from pymongo import MongoClient



client = MongoClient()



dbfile = sys.argv[1]
mongodbname = sys.argv[2] if len(sys.argv) > 2 else 'gnucash'
mdb = client[mongodbname]

db = sqlite3.connect(dbfile)
c  = db.cursor()


if not os.path.exists('exports'):
    os.makedirs('exports')


q = "SELECT tbl_name FROM sqlite_master WHERE type='table'"
tbls = [t[0] for t in c.execute(q) if not t[0].startswith('sqlite')]
for tbl in tbls:
    mdb[tbl].drop()

for tbl_name in tbls:
    cmd = "sqlite3 %s << EOF\n" % dbfile
    cmd += ".output exports/%s.csv\n" % tbl_name
    cmd += ".mode csv\n"
    cmd += ".header on\n"
    cmd += "select*from %s;\n" % tbl_name
    cmd += ".quit\n"
    cmd += "EOF\n"
    os.system(cmd)
    cmd = "mongoimport --collection %s --type csv --headerline --db %s exports/%s.csv" % (tbl_name, mongodbname, tbl_name)
    os.system(cmd)



# Replace GUIDs by ObjectIDs

# Map GUIDS to ObjectIDs
guids = defaultdict(dict)
for tbl in tbls:
    for doc in mdb[tbl].find():
        if 'guid' in doc:
            guids[tbl][doc['guid']] = doc['_id']

namemap = {
    'commodity_guid' : 'commodities',
    'budget_guid'    : 'budgets',
    'account_guid'   : 'accounts',
    'currency_guid'  : 'commodities',
    'tx_guid'        : 'transactions',
}

for tbl in tbls:
    for doc in mdb[tbl].find():
        modified = False
        toremove = False
        for k in doc.keys():
            if k.endswith('_guid'):
                tblmap = None
                if k == 'parent_guid':
                    tblmap = tbl
                elif k in namemap:
                    tblmap = namemap[k]
                else:
                    continue
                if doc[k] != '':
                    dk = str(doc[k])
                    if doc[k] in guids[tblmap]:
                        doc[k] = guids[tblmap][dk]
                        modified = True
                    else:
                        print "Warning! GUID not found in target table (%s): {%s: %s}" % (tblmap, k, doc[k])
                        # TODO: Add an option to remove the document conditionally
                        toremove = True

        if toremove:
            print "Removing document:", doc
            mdb[tbl].remove({'_id' : doc['_id']})
        if modified:
            newvals = {k : v for k, v in doc.items() if k.endswith('_guid')}
            mdb[tbl].update({'_id' : doc['_id']}, {'$set' : newvals})

# Rename _guid fields to remove that suffix
for tbl in tbls:
    for doc in mdb[tbl].find():
        keystorename = {}
        for k in doc.keys():
            if k.endswith('_guid'):
                keystorename[k] = k.replace('_guid', '')

        mdb[tbl].update({'_id' : doc['_id']}, {'$rename' : keystorename})

# Replace date fields
for tbl in tbls:
    for doc in mdb[tbl].find():
        newvals = {}
        modified = False
        for k in doc.keys():
            if k.endswith('_date'):
                dk = doc[k]
                if dk != '':
                    newvals[k] = datetime.strptime(str(doc[k]), "%Y%m%d%H%M%S")
                    modified = True

        if modified:
            mdb[tbl].update({'_id' : doc['_id']}, {'$set' : newvals})





