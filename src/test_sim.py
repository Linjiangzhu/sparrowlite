from util.simhashIndex import SimhashIndex
from bs4 import BeautifulSoup
from bs4.element import Comment
import os, glob, farmhash, re, json, simhash

def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def tokenize(text):
    soup = BeautifulSoup(text, "html.parser")
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    w_list = []
    for t in visible_texts:
        for w in re.findall(r"[a-zA-Z0-9']+", t.strip()):
            w_list.append(w)
    return w_list
def compute_simhash(tokens):
        shingles = [''.join(shingle) for shingle in simhash.shingle(''.join(tokens), 4)]
        hashes = [farmhash.hash64(s) for s in shingles]
        return simhash.compute(hashes)

sidx = SimhashIndex()

root = "../DEV/"




i = 0
cnt = 0
for dir in os.listdir(root):
    for file in glob.glob(f"{os.path.join(root, dir)}/*.json"):
        with open(file, encoding="utf-8") as f:
            json_obj = json.load(f)
        
        # extract raw html content
        raw = json_obj["content"]
        tokens = tokenize(raw)
        doc_sim = compute_simhash(tokens)
        find_doc = sidx.find(doc_sim)
        if len(find_doc) == 0:
            sidx.add(i, doc_sim)
            i += 1
        cnt += 1
        print("\r{:>6} files scanned, {:>6} files added, {:>6} sim".format(cnt, i, len(find_doc)), end="")
