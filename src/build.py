from util.indexBuilder import IIDXBuilder
from configparser import ConfigParser

if __name__ == "__main__":
    cp = ConfigParser()
    cp.read("config.ini")

    read_dir = cp["database"]["websites_dir"]
    write_dir = cp["database"]["database_dir"]
    ifmerge = cp["database"].getboolean("merge_chunk")

    builder = IIDXBuilder(read_dir)
    builder.build_index(write_dir)
    if ifmerge:
        IIDXBuilder.merge_chunk(write_dir)
    IIDXBuilder.build_forward_index(write_dir)