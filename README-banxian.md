## Banxian

### Introduction

Banxian

****

### New features:

#### 1 

New Trace to log metrics of block listening. (--ratio)

#### 2
Add allied nodes to keep connection and broadcast preferentially (--anconfigurl)

Note that disk copied from another disk would keep the same node key, pleas delete the nodekey file (`/datadrive/gethdata/geth/nodekey`) before the first starting of geth (geth will generate a new nodekey).

(New nodes created after 2019/1/28 has been remove the old nodekey)

#### 3 Emulation Engine
Add an emulator and a datalogger for the emulator.
##### Usage:

* **Emulation Logger**:

    There are two kinds of usage scenarios:
    
    1. Logging-In-Time: log the traces of txs and blocks in time to record their real arrival time:
        ` geth  ...  --emulatorlogger --emulatordir [path of log dir] `
    2. Logging the past: log the historical data of blocks and txs. Blocks' arrival time is taken from blocks' timestamp.
    Txs are taken as arriving between the current block and the parent block randomly. 
        ` geth  ...  --emulatorlogger --emulatordir [path of log dir] --generateemulatorlogfrom=[] --generateemulatorlogto=[]`

* **Emulator**:

    ` geth  ... --emulatordir [path of log dir] --emulatefile [relativePath] --emulatefrom [startNumber of a avaible block ]`

    There is a script which can extract the start block number from generated log files
        
        ```
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
        ```

#### New config

* --ratio value :       External ratio log configuration; 1 for open, 0 for close(default = 1)
* --anconfigurl url:    TOML file url from web to config allied nodes
(available url:  https://raw.githubusercontent.com/guozhongxin/resource/master/banxian/anconfig.toml)

##### Emulator flags

* --emulatordir=directory: base directory for emulator logger and emulate mode (default value: [datadir]/emulatorLog)
* --emulatorlogger: enable the emulator logger (can also be used in emulate mode)
* --emulatefrom=blockNumber: enable the emulate mode and set chain head in emulation to value
* --emulatefile=relativePath: the log file for emulate mode to read, relative to emulatordir


##### Emulator log generator flags

* --generateemulatorlogfrom=blockNumber: the start (inclusive) block 
* --generateemulatorlogto=blockNumber: the end (exclusive) block
 