from model import PresentPage
import json

def getDummyResult() -> list:
    dummy = []

    return dummy

# this function is only for test
def getTestCachePage() -> str:
    with open("../ANALYST/www_cs_uci_edu/3eca9c89f2d386807742af7f4f1f566cbc46e75ebd7baa16f05fc8a3b9e0acb2.json") as f:
        data = json.loads(f.read())
    return data["content"]