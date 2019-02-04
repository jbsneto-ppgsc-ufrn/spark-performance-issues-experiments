#!/bin/bash

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

for conf in `cat $DIR/scripts/configurations.lst`; do
	echo "Running all workloads with ${conf}..."
	for workload in `cat $DIR/scripts/workloads.lst`; do
    	echo "Running Workload ${workload} ..."	
    	cd ${DIR}/${workload}
    	sh scripts/run.sh ${conf}
    	echo "Workload ${workload} finished!"
    	cd ..
	done
done
	