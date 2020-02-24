import json
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
        