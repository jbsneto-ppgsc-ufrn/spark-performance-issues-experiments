#!/bin/bash

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

# Spark and Hadoop folders
SPARK_DIR=/usr/local/spark
HADOOP_DIR=/usr/local/hadoop

# Bin folders
SPARK_BIN=${SPARK_DIR}/bin
HADOOP_BIN=${HADOOP_DIR}/bin

HADOOP_HDFS=${HADOOP_BIN}/hdfs
HDFS_RM="${HADOOP_HDFS} dfs -rm -r"
SPARK_SUBMIT=${SPARK_BIN}/spark-submit

# SBT
SBT=sbt

# Performance Reports folder
PERFORMANCE_METRICS_REPORTS_FOLDER="/home/hduser/reports"

if [ ! -d ${PERFORMANCE_METRICS_REPORTS_FOLDER} ]; then
	mkdir -p ${PERFORMANCE_METRICS_REPORTS_FOLDER};
fi 

# Spark Cluster Master
MASTER="spark://10.7.40.42:7077"

HDFS_OUTPUT_FOLDER="/user/hduser/Output"

# Spark-Submit Configurations
MASTER_CONF="--master ${MASTER}"
LISTENER_CONF="--conf spark.extraListeners=br.ufrn.dimap.forall.spark.PerformanceMetricsListener"

LISTENER_REPORT_TYPE_CONF="--conf spark.performance.metrics.reports.type=all"

REPORTS_FOLDER_CONF="--conf spark.performance.metrics.reports.folder="

REPORTS_CONF_PROFILE_CONF="--conf spark.performance.metrics.reports.configurationProfile="

# Configurations:
EXECUTORS_CONF_1="--num-executors 3 --executor-memory 6g --executor-cores 8"
EXECUTORS_CONF_2="--num-executors 6 --executor-memory 3g --executor-cores 4"
EXECUTORS_CONF_3="--num-executors 12 --executor-memory 1536m --executor-cores 2"
EXECUTORS_CONF_4="--num-executors 24 --executor-memory 768m --executor-cores 1"
EXECUTORS_CONF_5="--num-executors 3 --executor-memory 6g --executor-cores 1"
EXECUTORS_CONF_6="--num-executors 3 --executor-memory 6g --executor-cores 2"
EXECUTORS_CONF_7="--num-executors 3 --executor-memory 6g --executor-cores 4"
EXECUTORS_CONF_8="--num-executors 3 --executor-memory 3g --executor-cores 8"
EXECUTORS_CONF_9="--num-executors 3 --executor-memory 1536m --executor-cores 8"
EXECUTORS_CONF_10="--num-executors 3 --executor-memory 768m --executor-cores 8"



REPORTS_FOLDER_1="${PERFORMANCE_METRICS_REPORTS_FOLDER}/conf1"
REPORTS_FOLDER_2="${PERFORMANCE_METRICS_REPORTS_FOLDER}/conf2"
REPORTS_FOLDER_3="${PERFORMANCE_METRICS_REPORTS_FOLDER}/conf3"
REPORTS_FOLDER_4="${PERFORMANCE_METRICS_REPORTS_FOLDER}/conf4"
REPORTS_FOLDER_5="${PERFORMANCE_METRICS_REPORTS_FOLDER}/conf5"
REPORTS_FOLDER_6="${PERFORMANCE_METRICS_REPORTS_FOLDER}/conf6"
REPORTS_FOLDER_7="${PERFORMANCE_METRICS_REPORTS_FOLDER}/conf7"
REPORTS_FOLDER_8="${PERFORMANCE_METRICS_REPORTS_FOLDER}/conf8"
REPORTS_FOLDER_9="${PERFORMANCE_METRICS_REPORTS_FOLDER}/conf9"
REPORTS_FOLDER_10="${PERFORMANCE_METRICS_REPORTS_FOLDER}/conf10"



CLUSTER_CONF_1="${MASTER_CONF} ${EXECUTORS_CONF_1} ${LISTENER_CONF} ${LISTENER_REPORT_TYPE_CONF} ${REPORTS_CONF_PROFILE_CONF}conf1 ${REPORTS_FOLDER_CONF}${REPORTS_FOLDER_1}"
CLUSTER_CONF_2="${MASTER_CONF} ${EXECUTORS_CONF_2} ${LISTENER_CONF} ${LISTENER_REPORT_TYPE_CONF} ${REPORTS_CONF_PROFILE_CONF}conf2 ${REPORTS_FOLDER_CONF}${REPORTS_FOLDER_2}"
CLUSTER_CONF_3="${MASTER_CONF} ${EXECUTORS_CONF_3} ${LISTENER_CONF} ${LISTENER_REPORT_TYPE_CONF} ${REPORTS_CONF_PROFILE_CONF}conf3 ${REPORTS_FOLDER_CONF}${REPORTS_FOLDER_3}"
CLUSTER_CONF_4="${MASTER_CONF} ${EXECUTORS_CONF_4} ${LISTENER_CONF} ${LISTENER_REPORT_TYPE_CONF} ${REPORTS_CONF_PROFILE_CONF}conf4 ${REPORTS_FOLDER_CONF}${REPORTS_FOLDER_4}"
CLUSTER_CONF_5="${MASTER_CONF} ${EXECUTORS_CONF_5} ${LISTENER_CONF} ${LISTENER_REPORT_TYPE_CONF} ${REPORTS_CONF_PROFILE_CONF}conf5 ${REPORTS_FOLDER_CONF}${REPORTS_FOLDER_5}"
CLUSTER_CONF_6="${MASTER_CONF} ${EXECUTORS_CONF_6} ${LISTENER_CONF} ${LISTENER_REPORT_TYPE_CONF} ${REPORTS_CONF_PROFILE_CONF}conf6 ${REPORTS_FOLDER_CONF}${REPORTS_FOLDER_6}"
CLUSTER_CONF_7="${MASTER_CONF} ${EXECUTORS_CONF_7} ${LISTENER_CONF} ${LISTENER_REPORT_TYPE_CONF} ${REPORTS_CONF_PROFILE_CONF}conf7 ${REPORTS_FOLDER_CONF}${REPORTS_FOLDER_7}"
CLUSTER_CONF_8="${MASTER_CONF} ${EXECUTORS_CONF_8} ${LISTENER_CONF} ${LISTENER_REPORT_TYPE_CONF} ${REPORTS_CONF_PROFILE_CONF}conf8 ${REPORTS_FOLDER_CONF}${REPORTS_FOLDER_8}"
CLUSTER_CONF_9="${MASTER_CONF} ${EXECUTORS_CONF_9} ${LISTENER_CONF} ${LISTENER_REPORT_TYPE_CONF} ${REPORTS_CONF_PROFILE_CONF}conf9 ${REPORTS_FOLDER_CONF}${REPORTS_FOLDER_9}"
CLUSTER_CONF_10="${MASTER_CONF} ${EXECUTORS_CONF_10} ${LISTENER_CONF} ${LISTENER_REPORT_TYPE_CONF} ${REPORTS_CONF_PROFILE_CONF}conf10 ${REPORTS_FOLDER_CONF}${REPORTS_FOLDER_10}"


CONF_TYPE="conf_1" #default

cluster_config(){
        if [ "${CONF_TYPE}" = "conf1" ] ; then
                echo "${CLUSTER_CONF_1}"
        elif [ "${CONF_TYPE}" = "conf2" ] ; then
                echo "${CLUSTER_CONF_2}"
        elif [ "${CONF_TYPE}" = "conf3" ] ; then
                echo "${CLUSTER_CONF_3}"
        elif [ "${CONF_TYPE}" = "conf4" ] ; then
                echo "${CLUSTER_CONF_4}"
        elif [ "${CONF_TYPE}" = "conf5" ] ; then
                echo "${CLUSTER_CONF_5}"
        elif [ "${CONF_TYPE}" = "conf6" ] ; then
                echo "${CLUSTER_CONF_6}"
        elif [ "${CONF_TYPE}" = "conf7" ] ; then
                echo "${CLUSTER_CONF_7}"
        elif [ "${CONF_TYPE}" = "conf8" ] ; then
                echo "${CLUSTER_CONF_8}"
        elif [ "${CONF_TYPE}" = "conf9" ] ; then
                echo "${CLUSTER_CONF_9}"
        elif [ "${CONF_TYPE}" = "conf10" ] ; then
                echo "${CLUSTER_CONF_10}"
        else
                echo "${CLUSTER_CONF_1}"
        fi
}

# Create the report folders:

if [ ! -d ${REPORTS_FOLDER_1} ]; then
	mkdir -p ${REPORTS_FOLDER_1};
fi 

if [ ! -d ${REPORTS_FOLDER_1}/csv ]; then
	mkdir -p ${REPORTS_FOLDER_1}/csv;
fi 

if [ ! -d ${REPORTS_FOLDER_1}/json ]; then
	mkdir -p ${REPORTS_FOLDER_1}/json;
fi 

if [ ! -d ${REPORTS_FOLDER_2} ]; then
	mkdir -p ${REPORTS_FOLDER_2};
fi

if [ ! -d ${REPORTS_FOLDER_2}/csv ]; then
	mkdir -p ${REPORTS_FOLDER_2}/csv;
fi

if [ ! -d ${REPORTS_FOLDER_2}/json ]; then
	mkdir -p ${REPORTS_FOLDER_2}/json;
fi
 
if [ ! -d ${REPORTS_FOLDER_3} ]; then
	mkdir -p ${REPORTS_FOLDER_3};
fi 

if [ ! -d ${REPORTS_FOLDER_3}/csv ]; then
	mkdir -p ${REPORTS_FOLDER_3}/csv;
fi 

if [ ! -d ${REPORTS_FOLDER_3}/json ]; then
	mkdir -p ${REPORTS_FOLDER_3}/json;
fi 

if [ ! -d ${REPORTS_FOLDER_4} ]; then
	mkdir -p ${REPORTS_FOLDER_4};
fi 

if [ ! -d ${REPORTS_FOLDER_4}/csv ]; then
	mkdir -p ${REPORTS_FOLDER_4}/csv;
fi 

if [ ! -d ${REPORTS_FOLDER_4}/json ]; then
	mkdir -p ${REPORTS_FOLDER_4}/json;
fi 

if [ ! -d ${REPORTS_FOLDER_5} ]; then
	mkdir -p ${REPORTS_FOLDER_5};
fi 

if [ ! -d ${REPORTS_FOLDER_5}/csv ]; then
	mkdir -p ${REPORTS_FOLDER_5}/csv;
fi 

if [ ! -d ${REPORTS_FOLDER_5}/json ]; then
	mkdir -p ${REPORTS_FOLDER_5}/json;
fi 

if [ ! -d ${REPORTS_FOLDER_6} ]; then
	mkdir -p ${REPORTS_FOLDER_6};
fi 

if [ ! -d ${REPORTS_FOLDER_6}/csv ]; then
	mkdir -p ${REPORTS_FOLDER_6}/csv;
fi 

if [ ! -d ${REPORTS_FOLDER_6}/json ]; then
	mkdir -p ${REPORTS_FOLDER_6}/json;
fi 

if [ ! -d ${REPORTS_FOLDER_7} ]; then
	mkdir -p ${REPORTS_FOLDER_7};
fi 

if [ ! -d ${REPORTS_FOLDER_7}/csv ]; then
	mkdir -p ${REPORTS_FOLDER_7}/csv;
fi 

if [ ! -d ${REPORTS_FOLDER_7}/json ]; then
	mkdir -p ${REPORTS_FOLDER_7}/json;
fi 

if [ ! -d ${REPORTS_FOLDER_8} ]; then
	mkdir -p ${REPORTS_FOLDER_8};
fi 

if [ ! -d ${REPORTS_FOLDER_8}/csv ]; then
	mkdir -p ${REPORTS_FOLDER_8}/csv;
fi 

if [ ! -d ${REPORTS_FOLDER_8}/json ]; then
	mkdir -p ${REPORTS_FOLDER_8}/json;
fi 

if [ ! -d ${REPORTS_FOLDER_9} ]; then
	mkdir -p ${REPORTS_FOLDER_9};
fi 

if [ ! -d ${REPORTS_FOLDER_9}/csv ]; then
	mkdir -p ${REPORTS_FOLDER_9}/csv;
fi 

if [ ! -d ${REPORTS_FOLDER_9}/json ]; then
	mkdir -p ${REPORTS_FOLDER_9}/json;
fi 

if [ ! -d ${REPORTS_FOLDER_10} ]; then
	mkdir -p ${REPORTS_FOLDER_10};
fi 

if [ ! -d ${REPORTS_FOLDER_10}/csv ]; then
	mkdir -p ${REPORTS_FOLDER_10}/csv;
fi 

if [ ! -d ${REPORTS_FOLDER_10}/json ]; then
	mkdir -p ${REPORTS_FOLDER_10}/json;
fi 