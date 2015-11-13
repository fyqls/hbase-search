#! /usr/bin/env bash
RUNHOME=`dirname "$0"`
RUNHOME=`cd "$bin">/dev/null; pwd`

JAVA=$JAVA_HOME/bin/java
BINDIR=`pwd`


CONFIG=${CONFIG}:conf/

for f in ${RUNHOME}/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

for f in ${RUNHOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

#for f in /usr/lib/hbase/lib/*.jar; do
#  CLASSPATH=${CLASSPATH}:$f;
#done

#echo $CLASSPATH

$JAVA -classpath "$CONFIG:$CLASSPATH" io.transwarp.search.SearchScannerBuilder $@
#$JAVA -classpath "$CLASSPATH" -jar search-1.0-SNAPSHOT-jar-with-dependencies.jar $@

