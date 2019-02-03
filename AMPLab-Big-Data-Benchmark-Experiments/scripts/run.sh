#!/bin/bash

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

. "${DIR}/../scripts/config.sh"

CONF_TYPE=$1
CONF=`cluster_config`

JAR=${DIR}/target/scala-2.11/AMPLab-Big-Data-Benchmark-Experiments-assembly-1.0.jar

PACKAGE="br.ufrn.dimap.forall.spark"

INPUT_RANKINGS_URL="hdfs://master:54310/user/hduser/BigBenchDataSet/rankings"
INPUT_USERVISITS_URL="hdfs://master:54310/user/hduser/BigBenchDataSet/uservisits"

for application in `cat $DIR/scripts/applications.lst`; do
    echo "Preparing ${application} ..."
    
    APP="${application}"
    CLASS="--class ${PACKAGE}.${APP}"
    ARGS=""
        
    if [ "${application}" = "JoinQueryDifferentPartitioners" ] ; then
        APP="JoinQueryDifferentPartitioners"
        OUTPUT_URL="hdfs://master:54310/user/hduser/Output/join-query-results"
        ARGS="$INPUT_RANKINGS_URL $INPUT_USERVISITS_URL $OUTPUT_URL"
    elif [ "${application}" = "JoinQuerySamePartitioners" ] ; then
        APP="JoinQuerySamePartitioners"
        OUTPUT_URL="hdfs://master:54310/user/hduser/Output/join-query-results"
        ARGS="$INPUT_RANKINGS_URL $INPUT_USERVISITS_URL $OUTPUT_URL"
    elif [ "${application}" = "AggregationQueryGroupByKey" ] ; then
        APP="AggregationQueryGroupByKey"
        OUTPUT_URL="hdfs://master:54310/user/hduser/Output/aggregation-query"
        ARGS="$INPUT_USERVISITS_URL $OUTPUT_URL"
    elif [ "${application}" = "AggregationQueryReduceByKey" ] ; then
        APP="AggregationQueryReduceByKey"
        OUTPUT_URL="hdfs://master:54310/user/hduser/Output/aggregation-query"
        ARGS="$INPUT_USERVISITS_URL $OUTPUT_URL"
    elif [ "${application}" = "DistinctUserVisitsPerPageReduceByKey" ] ; then
        APP="DistinctUserVisitsPerPageReduceByKey"
        OUTPUT_URL="hdfs://master:54310/user/hduser/Output/distinct-user-visits-per-page"
        ARGS="$INPUT_USERVISITS_URL $OUTPUT_URL"
    elif [ "${application}" = "DistinctUserVisitsPerPageAggregateByKey" ] ; then
        APP="DistinctUserVisitsPerPageAggregateByKey"
        OUTPUT_URL="hdfs://master:54310/user/hduser/Output/distinct-user-visits-per-page"
        ARGS="$INPUT_USERVISITS_URL $OUTPUT_URL"
    elif [ "${application}" = "ScanQueryRepartition" ] ; then
        APP="ScanQueryRepartition"
        OUTPUT_URL="hdfs://master:54310/user/hduser/Output/page-ranks"
        ARGS="$INPUT_RANKINGS_URL $OUTPUT_URL"
    elif [ "${application}" = "ScanQueryCoalesce" ] ; then
        APP="ScanQueryCoalesce"
        OUTPUT_URL="hdfs://master:54310/user/hduser/Output/page-ranks"
        ARGS="$INPUT_RANKINGS_URL $OUTPUT_URL"
    fi
    
    echo "Running ${application}"
	
	${SPARK_SUBMIT} ${CLASS} ${CONF} ${JAR} ${ARGS}
	
	echo "${application} Completed"
    
done