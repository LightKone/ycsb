FROM openjdk:8u212-jdk-stretch

ENV TABLE ycsb-buck
ENV S3HOST cloudserver-0
ENV S3PORT 8000
ENV S3ACCESSKEYID accessKey1
ENV S3SECRETKEY verySecretKey1
ENV PROTEUSHOST qpuindex
ENV PROTEUSPORT 50250
ENV TYPE load
ENV WORKLOAD workloada
ENV RECORDCOUNT 1000
ENV INSERTSTART 0
ENV THREADS 1
ENV QUERYPROPORTION 1.0
ENV UPDATEPROPORTION 0.0
ENV CACHEDQUERYPROPORTION 0.0
ENV EXECUTIONTIME 60
ENV WARMUPTIME 0
ENV USEBARRIER false
ENV BARRIERNODE foo
ENV BARRIERMASTER foo
ENV BARRIERCLIENT1 foo
ENV BARRIERCLIENT2 foo

ENV OUTPUT_FILE_NAME out

ENV YCSB_DIR /app/ycsb
ENV MEASUREMENT_RESULTS_DIR /ycsb

RUN apt-get update && apt-get install -y \
  git \
  maven \
  nc \
  build-essential

COPY . $YCSB_DIR

ADD ./entrypoint.sh /

WORKDIR /app

# RUN git clone --single-branch --branch proteus https://github.com/dvasilas/YCSB.git
# RUN git clone https://github.com/dvasilas/proteus.git
RUN git clone https://github.com/vishnubob/wait-for-it.git

# RUN cd ./proteus/proteus_java_client && ./gradlew installDist
# RUN mkdir ./YCSB/s3/src/main/resources \
    # && cp ./proteus/proteus_java_client/build/libs/proteusclient.jar ./YCSB/s3/src/main/resources/

RUN curl https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-06.csv -o /yellow_tripdata_2019-06.csv

RUN mkdir ${MEASUREMENT_RESULTS_DIR}

VOLUME ${MEASUREMENT_RESULTS_DIR}

ENTRYPOINT ["/entrypoint.sh"]