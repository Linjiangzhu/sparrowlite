import json, os, sys, heapq, pickle
import numpy as np
from bs4 import BeautifulSoup
from bs4.element import Comment
from platform import system
from collections import namedtuple, defaultdict

def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size

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

    def feed(self, filepath: str) -> None:
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

class QueryDoc:
    def __init__(self, docid: str, termlist: [(str, float)], querys: [str], pagerank: float):
        self.docid = docid
        self.termlist = termlist
        self.querys = querys
        self.page_rank = pagerank
        self.cosine_sim = QueryDoc.cal_cosine_similarity(self)
        self.cumulative = QueryDoc.cal_cumulate_score(self)
        self.score = self.cumulative * self.cosine_sim * (self.page_rank * 50 + 2)

    def __lt__(self, other) -> bool:
        return self.score > other.score
    
    def __repr__(self) -> str:
        return  "{" + "doc :{:>6}, cumulative: {:.2f}, cosine sim: {:.2f}, page rank: {:.4e},  total score {:.2f}".format(
            self.docid, self.cumulative, self.cosine_sim, self.page_rank, self.score
        ) + "}"
    
    def __str__(self) -> str:
        return  "{" + "doc :{:>6}, cumulative: {:.2f}, cosine sim: {:.2f}, page rank: {:.4e},  total score {:.2f}".format(
            self.docid, self.cumulative, self.cosine_sim, self.page_rank, self.score
        ) + "}"

    @staticmethod
    def cal_cumulate_score(doc) -> float:
        return sum([e[1] for e in doc.termlist])

    @staticmethod
    def cal_cosine_similarity(doc) -> float:

        t_dict = {k: v for k, v in doc.termlist}
        q_list = list(set(doc.querys))
        q_vec = np.array([doc.querys.count(q) for q in q_list])
        t_vec = np.array([t_dict[q] if t_dict.get(q) != None else 0.0 for q in q_list])
        dot_product = np.dot(q_vec, t_vec)
        norm_q = np.linalg.norm(q_vec)
        norm_t = np.linalg.norm(t_vec)

        return dot_product / (norm_q * norm_t)

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

        # load term-termid into a dict
        with open(os.path.join(self.dir, "termid"), "r") as f:
            for line in f:
                k, v = line.strip().split(",")
                self.term_dict[k] = v

        # load docid-url into dict
        with open(os.path.join(self.dir, "docid"), "r") as f:
            for line in f:
                strip_l = line.strip()
                split = strip_l.find(",")
                k, v = strip_l[:split], strip_l[split + 1:]
                self.doc_dict[k] = v
        # load term-(start, length) into dict
        with open(os.path.join(self.dir, "fidx.csv"), "r") as f:
            for line in f:
                k, start, length = line.strip().split(",")
                self.fidx[k] = (start, length)

        # get the fixed length from inverted index line
        with open(os.path.join(self.dir, "out.csv"), "r") as f:
            line = f.readline()
            self.line_size = len(line.encode("utf-8"))
        
        # if file is build by windows, add 1 for carrage return
        self.seek_bock_size = self.line_size
        if system() == "Windows":
            self.seek_bock_size += 1
        
        # load docid-page ranke, append to docid-url dict
        page_rank = pickle.load(open(os.path.join(self.dir, "pagerank"), "rb"))
        for docid, url in self.doc_dict.items():
            if page_rank.get(docid) != None:
                self.doc_dict[docid] = (url, page_rank[docid])
            else:
                self.doc_dict[docid] = (url, 1.0)
        del page_rank

        # pint information of data loada
        print(f"[Data Info]")
        print(f"term: {len(self.term_dict)}\ndoc:{len(self.doc_dict)}\n")

    def find(self, w: str) -> [(str, str, float)]:
        if self.term_dict.get(w) == None:
            return []
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
            result.append((docid, w, float(score) * idf))
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

    # merge lists of documents        
    def merge_lists(self, lists: [list], querys:[str]) -> [QueryDoc]:
        if len(lists) == 0:
            return []
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
                        temp_idx = DB.binary_search(sorted_list[0], check_docid)
                        if temp_idx > idx:
                            idx = temp_idx
                        else:
                            idx += 1
                    break
                temp.append(tuple(sorted_list[i][sub_idx])[1:])            
                result.append(QueryDoc(check_docid, temp, querys, self.doc_dict[check_docid][1]))
            idx += 1

        return result

    # return list of document in which multiple query words are
    def select(self, querys: list, size=50) -> [str]:
        result_list = []
        for query in set(querys):
            if self.term_dict.get(query) != None:
                result_list.append(self.find(query))
        merged = self.merge_lists(result_list, querys)

        min_heap = []
        for doc in merged:
            if len(min_heap) < size:
                heapq.heappush(min_heap, doc)
            else:
                heapq.heappushpop(min_heap, doc)
        min_heap.sort()

        for r in min_heap:
            print(r)
        return [self.doc_dict[doc.docid][0] for doc in min_heap]
