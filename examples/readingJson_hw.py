__author__ = 'HowardW'


import json
from pprint import pprint

with open('tweets-1.json') as data_file:
    data = json.load(data_file)

for line in data:
    print line["created_at"] + " " +line["text"]