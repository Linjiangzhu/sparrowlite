import os, sys, json, glob, pickle
from collections import defaultdict
from util.textProcessor import TextProcessor

class IIDXBuilder:

    def __init__(self, dirpath):
        self.dirpath = dirpath
        self.term_dict = {}
        # self.term = set()
    
    # write dict to disk
    @staticmethod
    def write_partial_dict(d, filepath):
        with open(filepath, "w") as f:
            for k, v in sorted(d.items(), key=lambda x: x[0], reverse=False):
                for docid, score in sorted(v, key=lambda x: x[1], reverse=True):
                    f.write("{:>6},{:>5},{:>6}\n".format(k, docid, score))

    # compare the tf-idf score of two line
    # if same, compare the term id
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

    # merge two partial posting files
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
    
    # merge all files in the directory
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
                doc_id_file.write(f"{doc_id},{os.path.relpath(file, os.getcwd())}\n")
                doc_id += 1

                # progress bar
                print(f"\r{doc_id} files scanned       ", end="")

        # if buffer dict has remainig items, writ to disk
        if len(buffer_dict.keys()) != 0:
            IIDXBuilder.write_partial_dict(buffer_dict, os.path.join(outdir, f"{chunk_idx}.csv"))
        
        #  safe close files
        doc_id_file.close()
        term_id_file.close()