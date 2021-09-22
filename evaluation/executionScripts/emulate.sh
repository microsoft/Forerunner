/home/ae/forerunner/repo/Forerunner/build/bin/geth --datadir /mnt/ethereum  --datadir.ancient  /datadrive/ancient/ \
 --nousb --cache=40960  \
 --emulatordir=/datadrive/emulateLog \
 --emulatefile=$1 \
 --emulatefrom=$2 \
 --txpool.accountslots 1024 --txpool.globalslots 8192 --txpool.accountqueue 1024 --txpool.globalqueue 4096 \
 --perflog $3 $4 $5 $6 $7 $8 $9

