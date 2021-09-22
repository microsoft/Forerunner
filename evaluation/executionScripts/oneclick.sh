duration=$1

if [  "$duration" = "" ];
then 
	duration=3
fi
echo $duration "hours"
duration=$[duration*3600]


rm -rf /tmp/Perf*

nohup sleep $duration && /home/ae/forerunner/executionScripts/killgeth.sh  2>&1 &

/home/ae/forerunner/executionScripts/runEmulateBaseline.sh

cp /tmp/PerfTxLog.baseline.txt /home/ae/forerunner/perfScripts/

sleep 5

nohup sleep $duration && /home/ae/forerunner/executionScripts/killgeth.sh 2>&1 &

/home/ae/forerunner/executionScripts/runEmulateForerunner.sh

cp /tmp/PerfTxLog.reuse.txt /home/ae/forerunner/perfScripts/

python /home/ae/forerunner/perfScripts/join_perf.py -b /home/ae/forerunner/perfScripts/PerfTxLog.baseline.txt -f /home/ae/forerunner/perfScripts/PerfTxLog.reuse.txt -o /home/ae/forerunner/perfScripts/out

