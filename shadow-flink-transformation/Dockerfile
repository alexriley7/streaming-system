FROM flink:1.18.1

# Copy fat jar built by Maven
COPY target/shadow-flink-transformation-*.jar /opt/flink/usrlib/job.jar

# Optional: better logging
ENV FLINK_PROPERTIES="jobmanager.rpc.address: localhost"