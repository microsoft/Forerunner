import json
import sys
from collections import defaultdict

path = sys.argv[1] 

Blocks  = 17
Txs     = 97
TxPool  = 23

has_pool = False
block = 0
with open(path, 'r') as f:
  for line in f:
    j = json.loads(line)
    t = j['type']
    if t == TxPool: has_pool = True
    elif t == Blocks:
       if True and has_pool: 
         block = j['blocks'][0]['header']['number']
         break

print(int(block, 16) - 1)
