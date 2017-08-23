#!/bin/bash

sbin=`dirname "${BASH_SOURCE-$0}"`
sbin=`cd "$sbin"; pwd`
HOME_DIR="$sbin"/..

. $HOME_DIR/conf/hdfs-over-ftp-env.sh

JAVA_OPTS="-Xmx1024M"

if [ -z "$PID_FILE" ]; then
  PID_FILE="$HOME_DIR/run/hdfs-over-ftp.pid"
fi

if [ -z "$LOG_FILE" ]; then
  LOG_FILE="$HOME_DIR/logs/hdfs-over-ftp.log"
fi

JAR="$HOME_DIR/hdfs-over-ftp.jar"
JAVA_CMD="$JAVA_HOME/bin/java"

CLASSPATH=$HADOOP_CONF_DIR:$JAR
for f in $HOME_DIR/lib/*.jar;do
  CLASSPATH=${CLASSPATH}:$f;
done

export CLASSPATH=$CLASSPATH
CLASS=org.apache.hadoop.contrib.ftp.HdfsOverFtpServer

CONF_FILE="$HOME_DIR/conf/hdfs-over-ftp.properties"
USER_FILE="$HOME_DIR/conf/users.properties"

command="hdfs-over-ftp"
usage="Usage: hdfs-over-ftp.sh (start|stop)"
cmd=$1

case $cmd in

  (start)

    if [ -f $PID_FILE ]; then
     if kill -0 `cat $PID_FILE` > /dev/null 2>&1; then
        echo $command running as process `cat $PID_FILE`.  Stop it first.
        exit 1
     fi
    fi

    echo starting $command
      nohup $JAVA_CMD ${JAVA_OPTS} -Dconf.file=$CONF_FILE -Dusers.file=$USER_FILE $CLASS >> $LOG_FILE 2>&1 < /dev/null & echo $! > $PID_FILE
    ;;

  (stop)

    if [ -f $PID_FILE ]; then
      if kill -0 `cat $PID_FILE` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $PID_FILE`
		rm $PID_FILE
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;
esac
