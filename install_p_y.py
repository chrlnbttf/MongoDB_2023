from pymongo import MongoClient

from pprint import pprint

import re

from datetime import datetime

host="localhost",
port = 27017

client = MongoClient(
    host="127.0.0.1",
    port = 27017,
    username='admin',
    password='pass'
)

sample = client["sample"]
books = client["sample"]["books"]
