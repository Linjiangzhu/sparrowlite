import re, math, simhash, farmhash
from bs4 import BeautifulSoup
from bs4.element import Comment
from collections import defaultdict
from nltk.stem import PorterStemmer

class TextProcessor:
    def __init__(self):
        self.raw = ""
        self.text = ""
        self.token_dict = {}
        self.simhash = 0

    @staticmethod
    def isVisibleTag(element) -> bool:
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
        soup = BeautifulSoup(s, "html.parser")
        visible_html = soup.findAll(text=True)
        visible_text = filter(TextProcessor.isVisibleTag, visible_html)
        w_list = []
        for t in visible_text:
            textContent = str(t).strip()
            if textContent != "":
                for w in re.findall(r"[a-zA-Z0-9']+", textContent):
                    w_list.append(w.lower())
        return w_list

    def feed(self, content: str):
        soup = BeautifulSoup(content, "html.parser")
        self.token_dict = defaultdict(float)
        token_list = TextProcessor.tokenize(content)
        self.simhash = self.computeSimHash(token_list)

        ps = PorterStemmer()
        for w in token_list:
            self.token_dict[ps.stem(w)] += 1
        
        # handle strong
        for e in soup.find_all("strong"):
            for w in re.findall(r"[a-zA-Z0-9']+", e.get_text()):
                self.token_dict[ps.stem(w.lower())] += 10

        # handle h3
        for e in soup.find_all("h3"):
            for w in re.findall(r"[a-zA-Z0-9']+", e.get_text()):
                self.token_dict[ps.stem(w.lower())] += 10**2

        # handle h2
        for e in soup.find_all("h2"):
            for w in re.findall(r"[a-zA-Z0-9']+", e.get_text()):
                self.token_dict[ps.stem(w.lower())] += 10**3

        # handle h1
        for e in soup.find_all("h1"):
            for w in re.findall(r"[a-zA-Z0-9']+", e.get_text()):
                self.token_dict[ps.stem(w.lower())] += 10**4

        # handle title
        if soup.title != None and soup.title.string != None:
            for w in re.findall(r"[a-zA-Z0-9']+", soup.title.string):
                self.token_dict[ps.stem(w.lower())] += 10**5

        # calculate tf score
        for k, v in self.token_dict.items():
            self.token_dict[k] = TextProcessor.calTFScore(v)

        self.raw = ""
        self.text = ""

    def getTokenDict(self) -> dict:
        return self.token_dict
    
    def computeSimHash(self, tokens) -> int:
        shingles = [''.join(shingle) for shingle in simhash.shingle(''.join(tokens), 4)]
        hashes = [farmhash.hash64(s) for s in shingles]
        return simhash.compute(hashes)

    def getSimHash(self) -> int:
        return self.simhash
    
    def getlink(self, content) -> [(str, list)]:
        soup = BeautifulSoup(content, "html.parser")
        links = []
        ps = PorterStemmer()
        for link in soup.find_all('a'):
            w_set = set()
            href = link.get('href')
            title = link.get('title')
            inner_text = link.get_text()

            if title != None:
                for w in re.findall(r"[a-zA-Z0-9]+", title):
                    w_set.add(ps.stem(w.lower()))
            
            if inner_text != None:
                for w in re.findall(r"[a-zA-Z0-9]+", inner_text):
                    w_set.add(ps.stem(w.lower()))
            if href != None:
                links.append((href, list(w_set)))
        return links
        
