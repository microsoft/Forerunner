# How to evaluate Forerunner:

## 1. Preparation

Please download all the following into your machine first. 

* [Source code](https://github.com/microsoft/Forerunner).

* [Execution scripts](https://github.com/microsoft/Forerunner/tree/master/evaluation/executionScripts)

* [Performance evaluation scripts](https://github.com/microsoft/Forerunner/tree/master/evaluation/perfScripts)

* Workload data (03/12/2021 - 03/22/2021): 
    
    * https://forerunnerdata.blob.core.windows.net/workload/20210312.json?sp=r&st=2021-08-20T11:28:15Z&se=2022-08-20T19:28:15Z&spr=https&sv=2020-08-04&sr=b&sig=nFJx0FWVTnhF3NYJ147j5nR8jW3m5jpkAS7qr4sSdoM%3D
    
    * https://forerunnerdata.blob.core.windows.net/workload/20210313.json?sp=r&st=2021-08-20T11:28:51Z&se=2022-08-20T19:28:51Z&spr=https&sv=2020-08-04&sr=b&sig=aUICDJtl0zO%2FHwG%2FA8I5VgzE%2BKx3gKqgzEbU3sazkYM%3D

    * https://forerunnerdata.blob.core.windows.net/workload/20210314.json?sp=r&st=2021-08-20T11:29:07Z&se=2022-08-20T19:29:07Z&spr=https&sv=2020-08-04&sr=b&sig=vCSHW7LZxBPKhSXyDX8mmbwpTyYJfY4R%2BpE5%2BR5WaIQ%3D

    * https://forerunnerdata.blob.core.windows.net/workload/20210315.json?sp=r&st=2021-08-20T11:29:20Z&se=2022-08-20T19:29:20Z&spr=https&sv=2020-08-04&sr=b&sig=L8vgJvjaRe9WcxqZsN94JnpxGIP8%2BwyqbYYRNTYeUFo%3D

    * https://forerunnerdata.blob.core.windows.net/workload/20210316.json?sp=r&st=2021-08-20T11:29:34Z&se=2022-08-20T19:29:34Z&spr=https&sv=2020-08-04&sr=b&sig=FqQ5lfQ9qdOv2TwyXuCe0ZFT6eOg%2FRghPcRTyn%2FIoiI%3D

    * https://forerunnerdata.blob.core.windows.net/workload/20210317.json?sp=r&st=2021-08-20T11:29:52Z&se=2022-08-20T19:29:52Z&spr=https&sv=2020-08-04&sr=b&sig=1rMjzCzE4baHDfcRU1x1QeECICln4448%2FUHXGN13jYk%3D

    * https://forerunnerdata.blob.core.windows.net/workload/20210318.json?sp=r&st=2021-08-20T11:30:19Z&se=2022-08-20T19:30:19Z&spr=https&sv=2020-08-04&sr=b&sig=wbqV6jdq3ZL3kLeIzym%2FAa8nxcwFhmgooio42gSIAvQ%3D

    * https://forerunnerdata.blob.core.windows.net/workload/20210319.json?sp=r&st=2021-08-20T11:30:33Z&se=2022-08-20T19:30:33Z&spr=https&sv=2020-08-04&sr=b&sig=cZVW6D2MNe1ztgf8%2BngRBgmHEC5eemDMEA%2BC%2BG2Le8o%3D

    * https://forerunnerdata.blob.core.windows.net/workload/20210320.json?sp=r&st=2021-08-20T11:30:48Z&se=2022-08-20T19:30:48Z&spr=https&sv=2020-08-04&sr=b&sig=C4Z15vigV8FXO5%2FvTC6ZdQmJqW9%2Bifp4J6sW7UeXrS8%3D

    * https://forerunnerdata.blob.core.windows.net/workload/20210321.json?sp=r&st=2021-08-20T11:31:00Z&se=2022-08-20T19:31:00Z&spr=https&sv=2020-08-04&sr=b&sig=aWAFhm3vf788jOhBvaVFtbmzaPE2yCCp44qH%2BLev0Yk%3D

    * https://forerunnerdata.blob.core.windows.net/workload/20210322.json?sp=r&st=2021-08-20T11:31:21Z&se=2022-08-20T19:31:21Z&spr=https&sv=2020-08-04&sr=b&sig=sfITBLKbTKON4x0cgdKGIWm86Zh0vM4m7cK2hZAIFIg%3D

* Ethereum chain data and state data (for emulation): 
    * ethereum chaindata directory: datadir data (splited into three parts due to data size limit. Please concatenate them into one file and unzip it by 'tar -zxf <file>'): 		
        * https://forerunnerdata.blob.core.windows.net/ethdata/geth.tar.part_00?sp=r&st=2021-09-22T08:47:50Z&se=2022-12-31T16:47:50Z&spr=https&sv=2020-08-04&sr=b&sig=1VkDX4xpJQdpf8bEdQbCYa%2FNT7diBcdCsQ8xAhGx8yY%3D
		
		* https://forerunnerdata.blob.core.windows.net/ethdata/geth.tar.part_01?sp=r&st=2021-09-22T08:49:06Z&se=2022-12-31T16:49:06Z&spr=https&sv=2020-08-04&sr=b&sig=xhvMuZ0QWK7KKjVde6dSSS1X08Qvk3%2BTJTR0uY32JG4%3D
		
		* https://forerunnerdata.blob.core.windows.net/ethdata/geth.tar.part_02?sp=r&st=2021-09-22T08:49:28Z&se=2022-12-31T16:49:28Z&spr=https&sv=2020-08-04&sr=b&sig=ZPwLdkbT63NyKj%2B8M%2F6eX0Hnqzt%2F%2BJIMqyHktDrWgKQ%3D

    * ethereum chaindata ancient data directory: [datadir.ancient](https://forerunnerdata.blob.core.windows.net/ethdata/ancient.tar?sp=r&st=2021-09-22T08:46:48Z&se=2022-12-31T16:46:48Z&spr=https&sv=2020-08-04&sr=b&sig=uAAPZ8VecklwRvXUoy3YSrxE7TXAl6XEHT6X1SRgOBk%3D) data. (Please unzip it by 'tar -zxf <file>') 


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