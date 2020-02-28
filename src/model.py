import json, os
from bs4 import BeautifulSoup
from bs4.element import Comment

class PresentPage:
    def __init__(self, title: str, url: str, fpath: str, content: str):
        self.title = title
        self.url = url
        self.cache_path = fpath # for present cached page 
        self.content = content

class FileProcesser:
    def __init__(self):
        self.json = dict()
        self.soup = BeautifulSoup()
        self.title = ""
        self.url = ""
        self.content = ""

    def isVisibleTag(self, element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True

    def feed(self, filepath: str):
        self.title = ""
        self.url = ""
        self.content = ""
        with open(filepath, encoding="utf-8") as f:
            self.json = json.loads(f.read())
            self.soup = BeautifulSoup(self.json["content"], "html.parser")
            visible_raw = self.soup.findAll(text=True)
            visible_text = filter(self.isVisibleTag, visible_raw)
            self.content = u" ".join(t.strip() for t in visible_text)
            if self.soup.title != None:
                self.title = self.soup.title.string
            else:
                self.title = self.json["url"]
            self.url = self.json["url"]

    def getURL(self) -> str:
        return self.url

    def getTitle(self) -> str:
        return self.title

    def getContent(self) -> str:
        return self.content

class DB:

    def __init__(self, dir):
        self.dir = dir
        self.load()
    
    def load(self): 
        self.term_dict = {}
        self.doc_dict = {}
        self.fidx = {}
        self.line_size = 0

        with open(os.path.join(self.dir, "termid"), "r") as f:
            for line in f:
                k, v = line.strip().split(",")
                self.term_dict[k] = v

        with open(os.path.join(self.dir, "docid"), "r") as f:
            for line in f:
                k, v = line.strip().split(",")
                self.doc_dict[k] = v

        with open(os.path.join(self.dir, "fidx.csv"), "r") as f:
            for line in f:
                k, start, length = line.strip().split(",")
                self.fidx[k] = (start, length)
        
        with open(os.path.join(self.dir, "out.csv"), "r") as f:
            line = f.readline()
            self.line_size = len(line.encode("utf-8"))

    def get(self, w):
        term_idx = self.term_dict[w]
        start, length = (int(self.fidx[term_idx][0]), int(self.fidx[term_idx][1]))
        raw = ""
        result = []
        # print(start, length)
        # print("line size:", self.line_size)
        # print(self.line_size * start)
        with open(os.path.join(self.dir, "out.csv"), "r") as f:
            f.seek(start * (self.line_size + 1))
            raw = f.read(length * (self.line_size))
        for line in raw.splitlines():
            termid, docid, score = line.split(",")
            result.append(self.doc_dict[docid.strip()])
        return result
            
