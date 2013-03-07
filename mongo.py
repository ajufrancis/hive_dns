#!/usr/bin/env python

import pymongo
import datetime

connection = pymongo.Connection('192.168.105.128', 27017)
db = connection.hive_database

post = {"author": "Mike",
        "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
        "date": datetime.datetime.utcnow()}

posts = db.posts
posts.insert(post)

for post in posts.find():
    print post['text']


a = posts.find_one({"author": "Mike"})
print "find one",a['text']

print "delete it"
posts.remove({"author": "Mike"})
