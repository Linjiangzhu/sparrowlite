import os, sys, json, glob, pickle
from collections import defaultdict
from util.textProcessor import TextProcessor

class IIDXBuilder:

    def __init__(self, dirpath):
        self.dirpath = dirpath
        self.doc_dict = {}
        self.term_dict = {}
        self.term = set()
    
    def update_dict(self, main_dict, sub_dict, docid):
        for k, v in sub_dict.items():
            if k not in self.term:
                term_id = len(self.term)
                self.term.add(k)
                self.term_dict[k] = term_id
            else:
                term_id = self.term_dict[k]
            main_dict[term_id].append((docid, v))
    
    @staticmethod
    def get_dict_byte(d):
        size = 0
        size += sum([sys.getsizeof(k) for k in d.keys()])
        size += sum([sys.getsizeof(v) for v in d.values()])
        return size

    @staticmethod
    def write_partial_dict(d, filepath):
        with open(filepath, "w") as f:
            for k, v in sorted(d.items(), key=lambda x: x[0], reverse=False):
                for docid, score in sorted(v, key=lambda x: x[1], reverse=True):
                    f.write(f"{k},{docid},{score}\n")

    @staticmethod
    def compare_line(l1, l2):
        t1, _, s1 = l1.split(",")
        t2, _, s2 = l2.split(",")
        s1 = float(s1)
        s2 = float(s2)
        if t1 == t2:
            return s1 >= s2
        else:
            return t1 < t2

    @staticmethod
    def merge_two(f1path, f2path, outpath):
        f1 = open(f1path, "r")
        f2 = open(f2path, "r")
        outfile = open(outpath, "w")
        line_a = f1.readline().rstrip()
        line_b = f2.readline().rstrip()
        while line_a != "" and line_b != "":
            if IIDXBuilder.compare_line(line_a, line_b):
                outfile.write(line_a + '\n')
                line_a = f1.readline().rstrip()
            else:
                outfile.write(line_b + '\n')
                line_b = f2.readline().rstrip()
        if line_a != "":
            outfile.write(line_a + '\n')
            for line in f1.readlines():
                outfile.write(line)
        if line_b != "":
            outfile.write(line_b + '\n')
            for line in f2.readlines():
                outfile.write(line)
        f1.close()
        f2.close()
        outfile.close()
    
    @staticmethod
    def merge_chunk(outdir):
        data_files = glob.glob(os.path.join(outdir, "*"))
        idx = len(data_files)
        while len(data_files) > 1:
            f1p = data_files.pop()
            f2p = data_files.pop()
            IIDXBuilder.merge_two(f1p, f2p, os.path.join(outdir, str(idx)))
            os.remove(f1p)
            os.remove(f2p)
            data_files = glob.glob(os.path.join(outdir, "*"))
            idx += 1

    def build_index(self, outdir, chunksize=4096):
        # create folder to hold data
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        
        chunk_idx = 0
        doc_id = 0
        
        root = self.dirpath
        buffer_dict = defaultdict(list)
        text_processor = TextProcessor()

        for dir in os.listdir(root):
            for file in glob.glob(f"{os.path.join(root, dir)}/*.json"):
                with open(file, encoding="utf-8") as f:
                    json_obj = json.load(f)
                
                raw = json_obj["content"]
                text_processor.feed(raw)
                self.update_dict(buffer_dict, text_processor.getTokenDict(), doc_id)
                self.doc_dict[file] = doc_id

                if IIDXBuilder.get_dict_byte(buffer_dict) / 1024 > chunksize:
                    IIDXBuilder.write_partial_dict(buffer_dict, os.path.join(outdir, str(chunk_idx)))
                    buffer_dict = defaultdict(list)
                    chunk_idx += 1
                doc_id += 1
                print(f"\r{doc_id} files scanned       ", end="")

        if len(buffer_dict.keys()) != 0:
            IIDXBuilder.write_partial_dict(buffer_dict, os.path.join(outdir, str(chunk_idx)))
        with open(os.path.join(outdir, "docid"), "wb") as f:
            f.write(pickle.dumps(self.doc_dict))
        with open(os.path.join(outdir, "termid"), "wb") as f:
            f.write(pickle.dumps(self.term_dict))