#   Forerunner Ethereum

This is the code for reproducing the results of the SOSP 2021 paper **"Forerunner: Constraint-based Speculative Transaction Execution for Ethereum"** by Yang Chen (Microsoft Research), Zhongxin Guo (Microsoft Research), Runhuai Li (Zhejiang University and Microsoft Research), Shuo Chen (Microsoft Research), Lidong Zhou (Microsoft Research), Yajin Zhou (Zhejiang University), and Xian Zhang (Microsoft Research)

## How to build:  

The following is building steps. Users can also reference the build method of go-ethereum(https://github.com/ethereum/go-ethereum/blob/master/README.md) 

1. Install golang : https://golang.org/doc/install  (v1.13 or later) 

2. Install dependencies, including a C compiler: For ubuntu machines, you can run `sudo apt-get install build-essential` 

3. In the folder of source code, run `make geth`.  

Then, you can find the binary `geth` in the path `$projectDir/build/bin`. 

## How to execute Forerunner Ethereum

Basically, Forerunner Ethereum is compatible with all features of go-ethereum v1.9.9, and Forerunner Ethereum can be used as the same as go-ethereum v1.9.9.  

To enable features of Forerunner Ethereum mentioned in the paper and get the best performance, users just need to append a few flags to geth command line: 
```
--preplay --cmpreuse --parallelhasher 16 --parallelbloom --no-overmatching --add-fastpath 
``` 

To log performance of tx processing in the critical path, add this flag: 
``` 
--perflog 
```

Forerunner Ethereum has an emulation feature which can log Ethereum network workload and reproduce the workload in emulation mode. 

To log Ethereum network workload, use the basic flags and : 

``` 
--emulatorlogger   --emulatordir <workload dir path> 
```

To emulate on a given workload, use the basic flags and: 

```
--emulatordir <workload dir path> --emulatefile <workload file name>  --emulatefrom <start blocknumber of emulation> 
```

### Examples: 

The following example command line is how we set the flags which are compatible with go-ethereum v1.9.9 in our SOSP paper for `Baseline` results 

```
$projectDir/build/bin/geth --datadir <geth data dir> --datadir.ancient  <geth ancient data dir> --nousb --cache=40960  --txpool.accountslots 1024 --txpool.globalslots 8192 --txpool.accountqueue 1024 --txpool.globalqueue 4096 --perflog 
```

The following example command line is how we set the flags for `Forerunner` results in our SOSP paper.

```
$projectDir/build/bin/geth --datadir <geth data dir> --datadir.ancient  <geth ancient data dir> --nousb --cache=40960  --txpool.accountslots 1024 --txpool.globalslots 8192 --txpool.accountqueue 1024 --txpool.globalqueue 4096 --perflog \ 
--preplay --cmpreuse --parallelhasher 16 --parallelbloom --no-overmatching --add-fastpath 
```

### Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
