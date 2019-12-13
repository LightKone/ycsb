#!/bin/bash

cd ${YCSB_DIR}
cp ${MEASUREMENT_RESULTS_DIR}/QUERY_${PREFIX}*.hdr .
cp ${MEASUREMENT_RESULTS_DIR}/FRESHNESS_LATENCY_${PREFIX}*.hdr .
./bin/ycsb load basic -p recordcount=0 -p parse=true -p parsePrefix=${PREFIX} -P workloads/workloada > ${MEASUREMENT_RESULTS_DIR}/${PREFIX}.txt