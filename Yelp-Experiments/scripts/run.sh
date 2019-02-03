#!/bin/bash

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

. "${DIR}/../scripts/config.sh"

CONF_TYPE=$1
CONF=`cluster_config`

JAR=${DIR}/target/scala-2.11/Yelp-Experiments-assembly-1.0.jar

INPUT_URL="hdfs://master:54310/user/hduser/Yelp/reviews"
OUTPUT_URL="hdfs://master:54310/user/hduser/Output/ngram-count"
N="3"

PACKAGE="br.ufrn.dimap.forall.spark"

for application in `cat $DIR/scripts/applications.lst`; do
    echo "Preparing ${application} ..."
    
    APP="${application}"
    ARGS=""
    
    if [ "${application}" = "NGramsCountNonPrePartitioned_3" ] ; then
        APP="NGramsCountNonPrePartitioned"
        ARGS="$INPUT_URL $OUTPUT_URL $N"
    elif [ "${application}" = "NGramsCountPrePartitioned_3" ] ; then
        APP="NGramsCountPrePartitioned"
        ARGS="$INPUT_URL $OUTPUT_URL $N"
    fi
        
    CLASS="--class ${PACKAGE}.${APP}"
    
    echo "Running ${application}"
	
	${SPARK_SUBMIT} ${CLASS} ${CONF} ${JAR} ${ARGS}
	
	echo "${application} Completed"
done