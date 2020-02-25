import os, json
from model import PresentPage, FileProcesser

def get_dummy_result() -> list:
    dummy = [
        "../ANALYST/www_cs_uci_edu/ff6386a56527801874c4fa418c3467f6062dcc82c0ea7dbcb5d5b773e847e05b.json",
        "../ANALYST/www_cs_uci_edu/fb25ac3aea8ba552b55c822a8fef95a8029aba51d0188c5c4f461cb472e0f489.json",
        "../ANALYST/www_cs_uci_edu/e5f576c31e66e42fb83ab6144d8f7c74e25bc11a94a76f3af66cfa41208f9be9.json"
    ]
    result = []
    freader = FileProcesser()
    for fpath in dummy:
        freader.feed(fpath)
        result.append(PresentPage(
            title = freader.getTitle(),
            url = freader.getURL(),
            fpath = fpath,
            content = freader.getContent()
        ))

    return result

# this function is only for test
def getTestCachePage() -> str:
    with open("../ANALYST/www_cs_uci_edu/3eca9c89f2d386807742af7f4f1f566cbc46e75ebd7baa16f05fc8a3b9e0acb2.json") as f:
        data = json.loads(f.read())
    return data["content"]
def getCachedPage(fpath: str) -> str:
    with open(fpath) as f:
        data = json.loads(f.read())
    return data["content"]