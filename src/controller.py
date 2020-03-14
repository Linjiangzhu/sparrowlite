import os, json
from model import PresentPage, FileProcesser, DB, get_size
from nltk.stem import PorterStemmer

db = DB("../DATA/")
ps = PorterStemmer()
print("Database size: {} MB".format(int(get_size(db) / 1024 ** 2)))

def getCachedPage(fpath: str) -> str:
    with open(fpath) as f:
        data = json.loads(f.read())

def getQuery(query_string: str) -> list:
    words_stem = [ps.stem(q.lower()) for q in query_string.split()]
    result = []
    for f in db.select(words_stem):
        result.append(PresentPage(
            title = "",
            url = f,
            fpath = "",
            content = ""
        ))
    return result
