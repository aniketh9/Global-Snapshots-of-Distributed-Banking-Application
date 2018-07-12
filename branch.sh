#!/bin/bash +vx
LIB_PATH=$"/home/yaoliu/src_code/local/libthrift-1.0.0.jar:/home/yaoliu/src_code/local/slf4j-log4j12-1.5.8.jar:/home/yaoliu/src_code/local/slf4j-api-1.5.8.jar:/home/yaoliu/src_code/local/log4j-1.2.14.jar"

BRANCH_NAME=$1
PORT=$2

java -classpath bin/branch_classes:$LIB_PATH DistributedBranch ${BRANCH_NAME} ${PORT} 
