import simhash
from collections import defaultdict

class SimhashIndex:
    def __init__(self, distance = 3):
        self.map = defaultdict(list)
        self.distance = distance

    def add(self, key: str, val: int):
        part = [((val >> 16 * i) & 0xffff) for i in range(4)]
        for p in part:
            self.map[p].append((key, val))

    def find(self, hash_value: int):
        result = set()
        part = [((hash_value >> 16 * i) & 0xffff) for i in range(4)]
        for p in part:
            if self.map.get(p) != None:
                for docid, sig in self.map[p]:
                    if simhash.num_differing_bits(sig, hash_value) <= self.distance:
                        result.add(docid)
        return list(result)
