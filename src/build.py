import psutil, os
from util.indexBuilder import IIDXBuilder
from configparser import ConfigParser

if __name__ == "__main__":
    cp = ConfigParser()
    cp.read("config.ini")

    read_dir = cp["DATABASE"]["websites_dir"]
    write_dir = cp["DATABASE"]["database_dir"]
    ifmerge = cp["DATABASE"].getboolean("merge_chunk")

    p = psutil.Process(os.getpid())
    p.nice(psutil.HIGH_PRIORITY_CLASS)

    builder = IIDXBuilder(read_dir)
    builder.build_index(write_dir)
    # builder.build_graph(write_dir)

    if ifmerge:
        IIDXBuilder.merge_chunk(write_dir)
    IIDXBuilder.build_forward_index(write_dir)