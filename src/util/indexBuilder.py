import os, sys, json, glob, pickle
from collections import defaultdict
from util.textProcessor import TextProcessor

class IIDXBuilder:

    def __init__(self, dirpath):
        self.dirpath = dirpath
        self.term_dict = {}
        self.term = set()
    
    def update_dict(self, main_dict, sub_dict, docid, term_file):
        for k, v in sub_dict.items():
            if k not in self.term:
                term_id = len(self.term)
                self.term.add(k)
                self.term_dict[k] = term_id
                term_file.write(f"{k},{term_id}\n")
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
                    f.write("{:>5},{:>5},{:>6}\n".format(k, docid, score))

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
        data_files = glob.glob(os.path.join(outdir, "*.csv"))
        idx = len(data_files)
        while len(data_files) > 1:
            f1p = data_files.pop()
            f2p = data_files.pop()
            IIDXBuilder.merge_two(f1p, f2p, os.path.join(outdir, f"{idx}.csv"))
            os.remove(f1p)
            os.remove(f2p)
            data_files = glob.glob(os.path.join(outdir, "*.csv"))
            idx += 1

    def build_index(self, outdir, chunksize=100000):
        # create folder to hold data
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        
        chunk_idx = 0
        doc_id = 0
        
        root = self.dirpath
        buffer_dict = defaultdict(list)
        text_processor = TextProcessor()
        chunk_sum = 0
        
        doc_id_file = open(os.path.join(outdir, "docid"), "a+")
        term_id_file = open(os.path.join(outdir, "termid"), "a+")
        for dir in os.listdir(root):
            for file in glob.glob(f"{os.path.join(root, dir)}/*.json"):
                with open(file, encoding="utf-8") as f:
                    json_obj = json.load(f)
                
                raw = json_obj["content"]
                text_processor.feed(raw)
                partial_dict = text_processor.getTokenDict()
                chunk_sum += len(partial_dict.keys())

                self.update_dict(buffer_dict, partial_dict, doc_id, term_id_file)
                doc_id_file.write(f"{doc_id},{os.path.relpath(file, os.getcwd())}\n")
                # self.doc_dict[doc_id] = os.path.relpath(file, os.getcwd())

                if chunk_sum > chunksize:
                    IIDXBuilder.write_partial_dict(buffer_dict, os.path.join(outdir, f"{chunk_idx}.csv"))
                    buffer_dict = defaultdict(list)
                    chunk_sum = 0
                    chunk_idx += 1
                doc_id += 1
                print(f"\r{doc_id} files scanned       ", end="")

        if len(buffer_dict.keys()) != 0:
            IIDXBuilder.write_partial_dict(buffer_dict, os.path.join(outdir, f"{chunk_idx}.csv"))
        doc_id_file.close()
        term_id_file.close()