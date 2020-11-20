FROM res-drl-hpc-docker-local.artifactory.swg-devops.com/spark:v3.0.0 AS build

USER root

ENV TPCDS_HOME=/opt/spark/tpc-ds-performance-test

RUN apt-get update && \
    apt-get -y install gcc make flex bison byacc git curl gnupg && \
    echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add && \
    apt-get update && \
    apt-get install sbt && \
    mkdir -p ${TPCDS_HOME} 

COPY . ${TPCDS_HOME}

RUN cd ${TPCDS_HOME} && \ 
    sbt package 

#Compile the TPC DS tools
#RUN cd ${TPCDS_HOME}/src/toolkit/tools && \
#    make clean && \
#    make OS=LINUX

FROM res-drl-hpc-docker-local.artifactory.swg-devops.com/spark:v3.0.0

ENV SPARK_HOME=/opt/spark
ENV TPCDS_HOME=/opt/spark/tpc-ds-performance-test

USER root
RUN mkdir -p ${SPARK_HOME}/work && \
    mkdir -p ${TPCDS_HOME} && \
    chown -R 185:185 ${SPARK_HOME}
COPY podtemplate.yaml ${TPCDS_HOME}
USER 185

#COPY . ${TPCDS_HOME}

#COPY --from=build /opt/spark/tpc-ds-performance-test/src/toolkit/tools ${TPCDS_HOME}/src/toolkit/tools/
COPY --from=build /opt/spark/tpc-ds-performance-test/target/scala-2.12/tpc-ds-benchmark-perf_2.12-0.1.jar /opt/spark/

WORKDIR ${SPARK_HOME}
#Configure tpcdsenv.sh
#We will statically define tpcdsenv.sh
#ENTRYPOINT [ "/opt/spark/tpc-ds-performance-test/bin/tpcdsspark.sh" ]
#CMD ["3"]