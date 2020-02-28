import os, sys, json, glob, pickle, heapq
from collections import defaultdict
from util.textProcessor import TextProcessor

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
        doc_id = 0
        chunk_idx = 0
        chunk_sum = 0
        root = self.dirpath
        buffer_dict = defaultdict(list)
        text_processor = TextProcessor()
        
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
                print(f"\r{doc_id} files scanned       ", end="")

        # if buffer dict has remainig items, writ to disk
        if len(buffer_dict.keys()) != 0:
            IIDXBuilder.write_partial_dict(buffer_dict, os.path.join(outdir, f"{chunk_idx}.csv"))
        
        #  safe close files
        doc_id_file.close()
        term_id_file.close()