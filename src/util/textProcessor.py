import re
import math
from bs4 import BeautifulSoup
from bs4.element import Comment
from collections import defaultdict
from nltk.stem import PorterStemmer

class TextProcessor:
    def __init__(self):
        self.raw = ""
        self.text = ""
        self.token_dict = {}

    def isVisibleTag(self, element) -> bool:
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True
    
    @staticmethod
    def calTFScore(wc: int) -> int:
        return round(1 + math.log10(wc), 4)
    
    @staticmethod
    def tokenize(s: str) -> list:
        return re.findall(r"[a-zA-Z0-9']+", s)

    def feed(self, content: str):
        self.raw = content
        self.token_dict = defaultdict(float)
        soup = BeautifulSoup(self.raw, "html.parser")

        # handle body text
        ps = PorterStemmer()
        visible_html = soup.findAll(text=True)
        visible_text = filter(self.isVisibleTag, visible_html)
        for t in visible_text:
            textContent = str(t).strip()
            if textContent != "":
                for w in re.findall(r"[a-zA-Z0-9']+", textContent):
                    self.token_dict[ps.stem(w.lower())] += 1
        
        # handle strong
        for e in soup.find_all("strong"):
            for w in re.findall(r"[a-zA-Z0-9']+", e.get_text()):
                self.token_dict[ps.stem(w.lower())] += 2

        # handle h3
        for e in soup.find_all("h3"):
            for w in re.findall(r"[a-zA-Z0-9']+", e.get_text()):
                self.token_dict[ps.stem(w.lower())] += 3

        # handle h2
        for e in soup.find_all("h2"):
            for w in re.findall(r"[a-zA-Z0-9']+", e.get_text()):
                self.token_dict[ps.stem(w.lower())] += 4

        # handle h1
        for e in soup.find_all("h1"):
            for w in re.findall(r"[a-zA-Z0-9']+", e.get_text()):
                self.token_dict[ps.stem(w.lower())] += 5

        # handle title
        if soup.title != None and soup.title.string != None:
            for w in re.findall(r"[a-zA-Z0-9']+", soup.title.string):
                self.token_dict[ps.stem(w.lower())] += 6

        # calculate tf score
        for k, v in self.token_dict.items():
            self.token_dict[k] = TextProcessor.calTFScore(v)

        self.raw = ""
        self.text = ""

    def getTokenDict(self) -> dict:
        return self.token_dict