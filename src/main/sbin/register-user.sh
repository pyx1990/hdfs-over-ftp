#!/bin/bash

sbin=`dirname "${BASH_SOURCE-$0}"`
sbin=`cd "$sbin"; pwd`
HOME_DIR="$sbin"/..

. $HOME_DIR/conf/hdfs-over-ftp-env.sh
JAVA_OPTS="-Xmx1024M"

JAR="$HOME_DIR/hdfs-over-ftp.jar"
JAVA_CMD="$JAVA_HOME/bin/java"

CLASSPATH=$HADOOP_CONF_DIR:$JAR
for f in $HOME_DIR/lib/*.jar;do
  CLASSPATH=${CLASSPATH}:$f;
done

CLASS=org.apache.hadoop.contrib.ftp.RegisterUser

$JAVA_CMD -classpath $CLASSPATH ${JAVA_OPTS} $CLASS $1 $2

