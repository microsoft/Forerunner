# How to evaluate Forerunner:

## 1. Preparation

Please download all the following into your machine first. 

* [Source code](https://github.com/microsoft/Forerunner).

* [Execution scripts](https://github.com/microsoft/Forerunner/tree/master/evaluation/executionScripts)

* [Performance evaluation scripts](https://github.com/microsoft/Forerunner/tree/master/evaluation/perfScripts)

* Workload data (03/12/2021 - 03/22/2021): 
    
    * https://forerunnerdata.blob.core.windows.net/workload/20210312.json
    
    * https://forerunnerdata.blob.core.windows.net/workload/20210313.json

    * https://forerunnerdata.blob.core.windows.net/workload/20210314.json

    * https://forerunnerdata.blob.core.windows.net/workload/20210315.json

    * https://forerunnerdata.blob.core.windows.net/workload/20210316.json

    * https://forerunnerdata.blob.core.windows.net/workload/20210317.json

    * https://forerunnerdata.blob.core.windows.net/workload/20210318.json

    * https://forerunnerdata.blob.core.windows.net/workload/20210319.json

    * https://forerunnerdata.blob.core.windows.net/workload/20210320.json

    * https://forerunnerdata.blob.core.windows.net/workload/20210321.json

    * https://forerunnerdata.blob.core.windows.net/workload/20210322.json

* Ethereum chain data and state data (for emulation): 
    * ethereum chaindata directory: datadir data (splited into three parts due to data size limit. Please concatenate them into one file and unzip it by 'tar -zxf <file>'): 		
        * https://forerunnerdata.blob.core.windows.net/ethdata/geth.tar.part_00
		
	* https://forerunnerdata.blob.core.windows.net/ethdata/geth.tar.part_01
		
	* https://forerunnerdata.blob.core.windows.net/ethdata/geth.tar.part_02

    * ethereum chaindata ancient data directory: [datadir.ancient](https://forerunnerdata.blob.core.windows.net/ethdata/ancient.tar) data. (Please unzip it by 'tar -zxf <file>') 


## 2. How to run Forerunner for evaluation 

First, you can reference [Readme](https://github.com/microsoft/Forerunner/blob/main/README.md) to build the binary. 

### 2.1 There is the execution command line for Baseline emulation:

```
<path to the binary>/geth --datadir <path to ethereum chaindata directory>  \
 --datadir.ancient <path to ethereum ancient data directory>  \
 --nousb --txpool.accountslots 1024 --txpool.globalslots 8192 --txpool.accountqueue 1024 --txpool.globalqueue 4096 \ 
 --cache <megabytes of memory allocated to blockchain data caching> \
 --emulatordir <path to workload data directory> \
 --emulatefile <workload file name> \
 --emulatefrom <the start blocknumber of emulation> \
 --perflog
```

The following configurations are inheriant from the official go-etheruem.

* --datadir <path to ethereum chaindata directory> : We provide the download link of this data in Preparation. 
* --datadir.ancient <path to ethereum ancient data directory> : We provide the download link of this data in the above.
* --nousb --txpool.accountslots 1024 --txpool.globalslots 8192 --txpool.accountqueue 1024 --txpool.globalqueue 4096 : these configurations can remain unchanged.
* --cache <megabytes of memory allocated to blockchain data caching> : the default value is 1024. We set it as 20480 in our evaluations.

The following configurations are designed for emulation:

* --emulatordir <path to workload data directory> : this path is '/datadrive/emulateLog/' of the given VMs. You can set it according to your local machines.
* --emulatefile <workload file name> : we provide download links to some workload data in the above. One data file contains one-day workload and the file name is the date of workload recorded. To emulate for longer durations, you can concatenate data files of multi continuous days to one single file and set it as 
* --emulatefile. In our evaluations, we concatenate 10-day workload files into 20210312-22.json located in '/datadrive/emulateLog/'.
* --emulatefrom <the start blocknumber of emulation> : This value is corresponding to the --emulatefile. You can get the value according to --emulatefile by a python script "find_first_block.py" provided in the [Execution scripts](https://github.com/microsoft/Forerunner/tree/master/evaluation/executionScripts). For example, if you set the --emulatefile as 20210320.json to evaluate the workload data of 3/22/2021, you can get the return value of python find_first_block.py 20210320.json and set it as `emulatefrom `.


### 2.2 There is the execution command line of Forerunner emulation:

```
<path to the binary>/geth --datadir <path to ethereum chaindata directory>  \
 --datadir.ancient <path to ethereum ancient data directory>  \
 --nousb --txpool.accountslots 1024 --txpool.globalslots 8192 --txpool.accountqueue 1024 --txpool.globalqueue 4096 \ 
 --cache <megabytes of memory allocated to blockchain data caching> \
 --emulatordir <path to workload data directory> \
 --emulatefile <workload file name> \
 --emulatefrom <the start blocknumber of emulation> \
 --perflog\ 
 --preplay --cmpreuse --parallelhasher 16 --parallelbloom --no-overmatching --add-fastpath
```

Besides the configurations mentioned in Baseline emulation, Forerunner emulation need several more configurations: ` --preplay --cmpreuse --parallelhasher 16 --parallelbloom --no-overmatching --add-fastpath`. Please keep them unchanged to enable Forerunner features.


## 3. Evaluation step by step

1. Run Baseline execution script: example: https://github.com/microsoft/Forerunner/tree/master/evaluation/executionScripts/runEmulateBaseline.sh;

2. Stop Baseline by <ctrl + c> after a certain period of time (e.g. 3 hours)

3. Collect the performance log of Baseline: copy /tmp/PerfTxLog.baseline.txt to <output dir path> 

4. Run Forerunner execution script: example: https://github.com/microsoft/Forerunner/tree/master/evaluation/executionScripts/runEmulateForerunner.sh;

5. Stop Forerunner by <ctrl + c> after a certain period of time (e.g. 3 hours)

6. Collect the performance log of Forerunner: copy /tmp/PerfTxLog.reuse.txt to <output dir path>

7. Run scripts to compute speedups:

    python [join_perf.py](https://github.com/microsoft/Forerunner/tree/master/evaluation/perfScripts/join_perf.py) -b <path to PerfTxLog.baseline.txt> -f <path to PerfTxLog.reuse.txt > -o <output dir path>

Then, you can find the result in <output dir path>/TxSpeedupResult.txt

Furthermore, we provide a one-click script to auto executes the above steps:

** [oneclick.sh](https://github.com/microsoft/Forerunner/tree/master/evaluation/executionScripts/oneclick.sh) <count of hours; 3 by default> **.

It will execute each of Baseline and Forerunner for your configured hours and generate the final results into <output dir path>/perfScripts/out/TxSpeedupResult.txt.


**Note**:

* Baseline and Forerunner should be executed in serial.

* We recommend that the execution of both Baseline and Forerunner lasts for 3 hours at least to get more reliable measurements. The main results of the paper were obtained by running Baseline and Forerunner 10 days each.

* As mentioned in Section 5.6 of our paper, Forerunner consumes ~67GB memory on average which is still unoptimized. We recommend you use a machine equipped with 128 GB memory to get rid of OOM during evaluation.
