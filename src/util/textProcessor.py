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

    def feed(self, content: str):
        self.raw = content
        self.token_dict = defaultdict(float)
        soup = BeautifulSoup(self.raw, "html.parser")
        ps = PorterStemmer()
        visible_html = soup.findAll(text=True)
        visible_text = filter(self.isVisibleTag, visible_html)
        for t in visible_text:
            textContent = str(t).strip()
            if textContent != "":
                for w in re.findall(r"[a-zA-Z0-9']+", textContent):
                    self.token_dict[ps.stem(w.lower())] += 1
        for k, v in self.token_dict.items():
            self.token_dict[k] = TextProcessor.calTFScore(v)
        self.raw = ""
        self.text = ""

    def getTokenDict(self) -> dict:
        return self.token_dict