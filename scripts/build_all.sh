#!/bin/bash

DIR=`dirname "$0"`
DIR=`cd "${DIR}/.."; pwd`

. "${DIR}/scripts/config.sh"

for workload in `cat $DIR/scripts/workloads.lst`; do
    echo "Building Workload ${workload} ..."
    
    cd ${DIR}/${workload}
    
    ${SBT} clean
    
    ${SBT} assembly
    
    echo "Workload ${workload} builded!"
    
    cd ..

done
