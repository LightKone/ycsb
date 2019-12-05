#!/bin/bash

if [ "$TYPE" = "run" ]; then
  ./wait-for-it/wait-for-it.sh --host=${PROTEUSHOST} --port=${PROTEUSPORT} --timeout=0
fi
./wait-for-it/wait-for-it.sh --host=${S3HOST} --port=${S3PORT} --timeout=0

cd ${YCSB_DIR}

chmod +x barrierMaster.sh
chmod +x barrierClient1.sh
chmod +x barrierClient2.sh

./bin/ycsb ${TYPE} s3 \
  -P ./workloads/${WORKLOAD} \
  -p table=${TABLE} \
  -p sleep=${SLEEP} \
  -p useBarrier=${USEBARRIER} \
  -p barrierNode=${BARRIERNODE} \
  -p barrierMaster=${BARRIERMASTER} \
  -p barrierClient1=${BARRIERCLIENT1} \
  -p barrierClient2=${BARRIERCLIENT2} \
  -threads ${THREADS} \
  -p maxexecutiontime=${EXECUTIONTIME} \
  -p warmuptime=${WARMUPTIME} \
  -p attributedataset=/yellow_tripdata_2019-06.csv \
  -p proteus.host=${PROTEUSHOST} \
  -p proteus.port=${PROTEUSPORT} \
  -p s3.endPoint=http://${S3HOST}:${S3PORT}  \
  -p s3.accessKeyId=${S3ACCESSKEYID} \
  -p s3.secretKey=${S3SECRETKEY} \
  -p recordcount=${RECORDCOUNT} \
  -p insertstart=${INSERTSTART} \
  -p queryproportion=${QUERYPROPORTION} \
  -p updateproportion=${UPDATEPROPORTION} \
  -p cachedqueryproportion=${CACHEDQUERYPROPORTION} \
  -s > ${MEASUREMENT_RESULTS_DIR}/${OUTPUT_FILE_NAME}.txt

if [ "$TYPE" = "run" ]; then
  cp QUERY.hdr ${MEASUREMENT_RESULTS_DIR}/${OUTPUT_FILE_NAME}.hdr
fi