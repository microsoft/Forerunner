# used to locate blocks inside emulator log

import json
import sys

path = sys.argv[1]

Blocks, Txs, TxPool = 17, 97, 23

has_pool = False
block = 0
with open(path, 'r') as f:
    for line in f:
        j = json.loads(line)
        t = j['type']
        if t == TxPool:
            has_pool = True
        elif t == Blocks:
            if True and has_pool:
                block = j['blocks'][0]['header']['number']
                break

print(int(block, 16) - 1)
