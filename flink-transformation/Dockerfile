FROM flink:1.18.1

# Copy fat jar built by Maven
COPY target/flink-transformation-*.jar /opt/flink/usrlib/job.jar

RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop

RUN cp /opt/flink/opt/flink-s3-fs-hadoop-1.18*.jar \
    /opt/flink/plugins/s3-fs-hadoop/

# Optional: better logging
ENV FLINK_PROPERTIES="jobmanager.rpc.address: localhost"