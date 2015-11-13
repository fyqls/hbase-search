#! /usr/bin/env bash
bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin">/dev/null; pwd`
HOME=$bin/../

#JAVA=$JAVA_HOME/bin/java
JAVA=/usr/bin/java

CONFIG=${CONFIG}:${HOME}/conf/

if [ ! -e $HOME/conf/hbase-site.xml ];then
  echo "please put a hbase-site.xml file under $HOME/conf/"
  exit 1
fi

for f in ${HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

#echo $CLASSPATH

$JAVA -classpath "$CONFIG:$CLASSPATH" io.transwarp.search.SearchScannerMain $@

