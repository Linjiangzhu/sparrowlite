from util.indexBuilder import IIDXBuilder

if __name__ == "__main__":
    # builder = IIDXBuilder("../ANALYST/")
    builder = IIDXBuilder("../DEV/")

    builder.build_index("../DATA/")
    # IIDXBuilder.merge_chunk("../DATA/")