import json, os, sys, heapq
import numpy as np
from numpy.linalg import norm
from bs4 import BeautifulSoup
from bs4.element import Comment
from platform import system
from collections import namedtuple
from sklearn.metrics.pairwise import cosine_similarity

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

# QueryDoc = namedtuple("QueryDoc", "docid termlist")
class QueryDoc:
    def __init__(self, docid: str, termlist: [(str, float)]):
        self.docid = docid
        self.termlist = termlist
    def __lt__(self, other) -> bool:
        return QueryDoc.cal_cumulate_score(self) > QueryDoc.cal_cumulate_score(other)
        # return QueryDoc.cal_cosine_similarity(self) > QueryDoc.cal_cosine_similarity(other)
    
    def __repr__(self) -> str:
        return  "{" + "doc :{}, cumulative: {}, cosine sim: {}".format(
            self.docid, QueryDoc.cal_cumulate_score(self), QueryDoc.cal_cosine_similarity(self)
        ) + "}"
    
    def __str__(self) -> str:
        return "{" + f"docid: {self.docid}" + "}"

    @staticmethod
    def cal_cumulate_score(doc) -> float:
        return sum([e[1] for e in doc.termlist])

    @staticmethod
    def cal_cosine_similarity(doc) -> float:
        sorted_list = sorted(doc.termlist, key=lambda x: x[0], reverse=False)
        vec = np.array([e[1] for e in sorted_list]).reshape(1, len(sorted_list))
        vec /= norm(vec)
        unit_vec = np.ones((1, len(sorted_list)))
        unit_vec /= norm(unit_vec)
        return cosine_similarity(vec, unit_vec)[0][0]

class DB:
    def __init__(self, dir: str):
        self.dir = dir
        self.load()
    
    def load(self) -> None: 
        self.term_dict = {}
        self.doc_dict = {}
        self.fidx = {}
        self.seek_bock_size = 0
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
        
        self.seek_bock_size = self.line_size
        if system() == "Windows":
            self.seek_bock_size += 1
        
        print(f"[Data Info]")
        print(f"term: {len(self.term_dict)}\ndoc:{len(self.doc_dict)}\n")
        

    # return list of document in which single query word is
    def get(self, w: str) -> [str]:
        term_idx = self.term_dict[w]
        start, length = (int(self.fidx[term_idx][0]), int(self.fidx[term_idx][1]))
        raw = ""
        result = []
        # print(start, length)
        # print("line size:", self.line_size)
        # print(self.line_size * start)
        with open(os.path.join(self.dir, "out.csv"), "r") as f:
            # f.seek(start * (self.line_size + 1))
            f.seek(start * (self.line_size + 0))
            raw = f.read(length * (self.line_size))

        for line in raw.splitlines():
            termid, docid, score = line.split(",")
            result.append(self.doc_dict[docid.strip()])
        return result
    
    def find(self, w: str) -> [(str, str, float)]:
        term_idx = self.term_dict[w]
        start, length = (int(self.fidx[term_idx][0]), int(self.fidx[term_idx][1]))
        raw = ""
        result = []
        idf = len(self.doc_dict) / length

        with open(os.path.join(self.dir, "out.csv"), "r") as f:
            f.seek(start * self.seek_bock_size)
            raw = f.read(length * self.line_size)
        for line in raw.splitlines():
            termid, docid, score = [e.strip() for e in line.split(",")]
            result.append((docid, termid, float(score) * idf))
        result = sorted(result, key=lambda x: x[0], reverse=False)
        return result

    @staticmethod
    def binary_search(arr: list, val: str) -> int:
        lo = 0
        hi = len(arr) - 1
        while lo <= hi:
            mid = int((lo + hi) / 2)
            if arr[mid][0] < val:
                lo = mid + 1
            elif arr[mid][0] > val:
                hi = mid -1
            else:
                return mid
        if hi < 0:
            return 0
        elif lo > len(arr) - 1:
            return -1
        else:
            return lo
        
    def merge_lists(self, lists: [list]) -> [QueryDoc]:
        idx = 0
        result = []
        sorted_list = sorted(lists, key=lambda x: len(x), reverse=False)

        while idx < len(sorted_list[0]):

            check_docid = sorted_list[0][idx][0]
            temp = []

            for i in range(len(sorted_list)):
                sub_idx = DB.binary_search(sorted_list[i], check_docid)
                if sub_idx == -1 or sorted_list[i][sub_idx][0] != check_docid:
                    temp = []
                    if sub_idx != -1:
                        check_docid = sorted_list[i][sub_idx][0]
                        idx = DB.binary_search(sorted_list[0], check_docid)
                    break
                temp.append(tuple(sorted_list[i][sub_idx])[1:])            

            if len(temp) != 0:
                result.append(QueryDoc(check_docid, temp))
            idx += 1

        return result

    # return list of document in which multiple query words are
    def select(self, querys: list, size=10) -> [str]:
        result_list = []
        for query in querys:
            if self.term_dict.get(query) != None:
                result_list.append(self.find(query))
        merged = self.merge_lists(result_list)

        min_heap = []
        for doc in merged:
            if len(min_heap) < size:
                heapq.heappush(min_heap, doc)
            else:
                heapq.heappushpop(min_heap, doc)
        min_heap.sort()
        print(min_heap)
        return [self.doc_dict[doc.docid] for doc in min_heap]
