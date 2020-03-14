import os, sys, json, glob, pickle, heapq, pickle
import numpy as np
from collections import defaultdict
from urllib.parse import urljoin
from util.textProcessor import TextProcessor
from util.simhashIndex import SimhashIndex


class Posting:
    def __init__(self, val, fid):
        self.val = val
        self.fid = fid

    def __lt__(self, other) -> bool:
        t1, _, s1 = self.val.split(",")
        t2, _, s2 = other.val.split(",")
        s1 = float(s1)
        s2 = float(s2)
        if t1 == t2:
            return s1 >= s2
        else:
            return t1 < t2

class IIDXBuilder:

    def __init__(self, dirpath):
        self.dirpath = dirpath
        self.term_dict = {}
    
    # write dict to disk
    @staticmethod
    def write_partial_dict(d, filepath):
        with open(filepath, "w") as f:
            for k, v in sorted(d.items(), key=lambda x: x[0], reverse=False):
                for docid, score in sorted(v, key=lambda x: x[1], reverse=True):
                    f.write("{:>6},{:>5},{:>6}\n".format(k, docid, score))

    # merge all files in the directory
    @staticmethod
    def merge_chunk(outdir):
        line_count = 0
        partial_files = glob.glob(os.path.join(outdir, "*.csv"))
        file_table = [open(f, "r") for f in partial_files]
        q = []
        for i in range(len(file_table)):
            line = file_table[i].readline()
            if line != "":
                heapq.heappush(q, Posting(line.rstrip(), i))
        final_merge = open(os.path.join(outdir, "out.csv"), "a+")
        print()
        while len(q) > 0:
            obj = heapq.heappop(q)
            final_merge.write(f"{obj.val}\n")
            line_count += 1
            line = file_table[obj.fid].readline()
            if line != "":
                heapq.heappush(q, Posting(line.rstrip(), obj.fid))
            else:
                file_table[obj.fid].close()
        print("\r{:>6} lines write to output csv".format(line_count),end="")
        
        # safe close open files
        final_merge.close()
        for of in file_table:
            if not of.closed:
                of.close()

    # a forward index for retrieval document list of term from disk
    @staticmethod
    def build_forward_index(outdir: str) -> None:
        iidx_file = os.path.join(outdir, "out.csv")
        if not os.path.exists(iidx_file):
            return
        fidx_file = os.path.join(outdir, "fidx.csv")
        line_cnt = 0
        term_cnt = 0
        prev = ""
        line_width = ""

        outfile = open(fidx_file, "w")

        with open(iidx_file, "r") as f:
            line = f.readline()
            prev = line.split(",")[0].strip()
            line_width = len(line)
            outfile.write("{},0,".format(prev))
        
        with open(iidx_file, "r") as infile:
            for line in infile:
                curr = line.split(",")[0].strip()
                if curr != prev:
                    outfile.write("{}\n{},{},".format(term_cnt, curr, line_cnt))
                    prev = curr
                    term_cnt = 0
                term_cnt += 1
                line_cnt += 1
        outfile.write("{}\n".format(term_cnt))
        outfile.close()


    # build inverted index
    def build_index(self, outdir, chunksize=100000):
        # create folder to hold data
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        
        # initialize counters and buffer
        doc_cnt = 0
        doc_id = 0
        chunk_idx = 0
        chunk_sum = 0
        root = self.dirpath
        buffer_dict = defaultdict(list)
        text_processor = TextProcessor()
        simhash_idx = SimhashIndex()
        
        # prepare docid-path file and term-id file 
        doc_id_file = open(os.path.join(outdir, "docid"), "a+")
        term_id_file = open(os.path.join(outdir, "termid"), "a+")

        # grab json files 
        for dir in os.listdir(root):
            for file in glob.glob(f"{os.path.join(root, dir)}/*.json"):
                with open(file, encoding="utf-8") as f:
                    json_obj = json.load(f)
                
                # extract raw html content
                raw = json_obj["content"]

                # feed to text processor
                text_processor.feed(raw)

                # get the simhash index of the web page
                page_simhash = text_processor.getSimHash()

                # if hammine distance is less than 3, ignore this document
                found_doc = simhash_idx.find(page_simhash)
                doc_cnt += 1
                if len(found_doc) == 0:
                    simhash_idx.add(doc_id, page_simhash)
                else:
                    print("\r{:>6} files scanned, {:>6} files added".format(doc_cnt, doc_id), end="")
                    continue

                # get word-freq dict of this file
                partial_dict = text_processor.getTokenDict()

                # iter through dict
                for k, v in partial_dict.items():
                    # if buffer dict reach chunksize, write to disk
                    if chunk_sum >= chunksize:
                        IIDXBuilder.write_partial_dict(buffer_dict, os.path.join(outdir, f"{chunk_idx}.csv"))

                        # empty buffer dict, update counters
                        buffer_dict = defaultdict(list)
                        chunk_sum = 0
                        chunk_idx += 1

                    # update file word-freq dict to buffer dict
                    if self.term_dict.get(k) == None:
                        # if word is new, give a new term id, write to term-id file
                        term_id = len(self.term_dict)
                        self.term_dict[k] = term_id
                        term_id_file.write(f"{k},{term_id}\n")
                    else:
                        # if not, get term id
                        term_id = self.term_dict[k]

                    # add to buffer dict, update counter
                    buffer_dict[term_id].append((doc_id, v))
                    chunk_sum += 1
                
                # write to docid-path file after process token
                # doc_id_file.write(f"{doc_id},{os.path.relpath(file, os.getcwd())}\n")
                web_url = json_obj["url"]
                doc_id_file.write(f"{doc_id},{web_url}\n")
                doc_id += 1

                # progress bar
                print("\r{:>6} files scanned, {:>6} files added".format(doc_cnt, doc_id), end="")

        # if buffer dict has remainig items, writ to disk
        if len(buffer_dict.keys()) != 0:
            IIDXBuilder.write_partial_dict(buffer_dict, os.path.join(outdir, f"{chunk_idx}.csv"))
        
        #  safe close files
        doc_id_file.close()
        term_id_file.close()

    @staticmethod
    def write_partial_set(d, filepath):
        with open(filepath, "w") as f:
            for k, v in sorted(d.items(), key=lambda x: x[0], reverse=False):
                for docid in sorted(v, key=lambda x: x, reverse=True):
                    f.write("{:>6},{:>5}\n".format(k, docid))
    
    def build_graph(self, outdir):
        cnt = 0
        doc_id_dict = dict()
        doc_id_to = dict()
        with open(os.path.join(outdir, "docid"), "r") as f:
            for line in f:
                strip_l = line.strip()
                split = strip_l.find(",")
                k, v = strip_l[:split], strip_l[split + 1:]
                    # k, v = line.strip().split(",")
                doc_id_dict[v] = k
                doc_id_to[k] = v
    
        root = self.dirpath
        text_processor = TextProcessor()
        graph = defaultdict(set)

        for dir in os.listdir(root):
            for file in glob.glob(f"{os.path.join(root, dir)}/*.json"):
                with open(file, encoding="utf-8") as f:
                    json_obj = json.load(f)
                raw = json_obj["content"]
                url = json_obj["url"]
                if doc_id_dict.get(url) != None:
                    links_list = text_processor.getlink(raw)
                    for href, _ in links_list:
                        link_addr = urljoin(url, href)
                        if doc_id_dict.get(link_addr) != None:
                                graph[doc_id_dict[url]].add(doc_id_dict[link_addr])
                print("\r{:>6} files scanned".format(cnt), end="")
                cnt += 1
        
        pickle.dump(graph, open(os.path.join(outdir, "graph"), "wb"))

        docs = set()
        for k, l in graph.items():
            docs.add(k)
            for v in l:
                docs.add(v)
        docs = sorted([i for i in docs], key=lambda x: int(x), reverse=False)
        doc_to_idx = {docs[i]: i for i in range(len(docs))}
        idx_to_docs = {i : docs[i] for i in range(len(docs))}

        size = len(docs)
        mtx = np.zeros((size, size))
        for k, v in graph.items():
            for c in v:
                mtx[int(k)][int(c)] = 1
        
        m = np.shape(mtx)[0]
        T = np.array(mtx, float)
        del mtx
        for i in range(m):
            s = np.sum(T[i])
            if s > 0:
                T[i] /= s
            else:
                T[i, i] = 1

        alpha = 0.2

        B = np.ones((m,m)) / m
        G = (1 - alpha) * T + alpha * B
        del B
        del T

        pi0 = np.ones((m,)) / m
        N = 100
        prob = np.zeros((N + 1, m))
        prob[0] = pi0
        for i in range(1, N + 1):
            prob[i] = np.matmul(prob[i-1], G)

        epsilon = np.zeros((N,))
        for i in range(1, N + 1):
            epsilon[i-1] = np.absolute(prob[i] - prob[i - 1]).sum()

        # indexes of page rank from high to low
        rank_idx = np.argsort(prob[-1])[::-1]

        # pagerank score from high to low
        rank_val = prob[-1][rank_idx]

        del G

        np.save(os.path.join(outdir, "rank_idx"),rank_idx)
        np.save(os.path.join(outdir, "rank_val"),rank_val)
        np.save(os.path.join(outdir, "epsilon"),epsilon)

        pagerank = {}
        for i in range(len(rank_idx)):
            pagerank[idx_to_docs[rank_idx[i]]] = rank_val[i]

        # serialize and save docid-page_rank_score to local
        pickle.dump(pagerank, open(os.path.join(outdir, "pagerank"), "wb"))
