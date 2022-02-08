#!/bin/sh

STARTTIME=$(date +%s)
ROWS=50000
ROWS4=$(($ROWS/4))
echo Starting...
touch ./_ts_

JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_271.jdk/Contents/Home/bin"
PATH=$JAVA_HOME:$PATH

java -Xmx2048m -Dfile.encoding=UTF-8 -classpath ./sapjco3.jar:./TableReader.jar TableReader S $ROWS4 0 > logTableReader_P1.log & 

sleep 1

java -Xmx2048m -Dfile.encoding=UTF-8 -classpath ./sapjco3.jar:./TableReader.jar TableReader S $ROWS4 $ROWS4 > logTableReader_P2.log & 

sleep 1

java -Xmx2048m -Dfile.encoding=UTF-8 -classpath ./sapjco3.jar:./TableReader.jar TableReader S $ROWS4 $(($ROWS/2)) > logTableReader_P3.log & 

sleep 1

java -Xmx2048m -Dfile.encoding=UTF-8 -classpath ./sapjco3.jar:./TableReader.jar TableReader S $ROWS4 $((3*$ROWS/4)) > logTableReader_P4.log & 

#!/bin/sh
p="a"
while [ ! -z "$p" ]
do
   clear
   #tput cup 0 0 
   # ps -lft|grep 'java\|Java\|PPID'|grep -v grep

   echo "*****************************************"
   echo "*  MULTI-PROCESS (4X) SAP TABLE READER  *"
   echo "*****************************************"
   ps -eo pid,%cpu,%mem,rss,args|grep 'TableReader\|ARGS'|grep -v grep
   p=$(ps -ef|grep TableReader|grep -v grep)
   find . -type f -newer _ts_ -name "outTableReader*" | xargs du -sh
   sleep 3
done

rm _ts_

ENDTIME=$(date +%s)
echo "********************************"
echo "Elapsed time: $(( $ENDTIME - $STARTTIME )) seconds"
echo "Rows: $ROWS"
echo "Throughput: $(( $ROWS/($ENDTIME - $STARTTIME) )) rows/sec"
echo "********************************"
