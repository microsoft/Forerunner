## Banxian

#### Introduction

Banxian

****

#### New features:

##### 1

New Trace to log metrics of block listening. (--ratio)

##### 2
Addd allied nodes to keep connection and broadcast preferentially (--anconfigurl)

Note that disk copied from another disk would keep the same node key, pleas delete the nodekey file (`/datadrive/gethdata/geth/nodekey`) before the first starting of geth (geth will generate a new nodekey).

(New nodes created after 2019/1/28 has been remove the old nodekey)


#### New config


* --ratio value :       External ratio log configuration; 1 for open, 0 for close(default = 1)
* --anconfigurl url:    TOML file url from web to config allied nodes
(available url:  https://raw.githubusercontent.com/guozhongxin/resource/master/banxian/anconfig.toml)
