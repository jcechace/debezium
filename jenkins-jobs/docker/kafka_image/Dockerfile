FROM registry.access.redhat.com/ubi8:latest

LABEL maintainer="Debezium QE"

#
# Set the version, home directory.
#
ARG KAFKA_VERSION=2.7.0 \
    ZK_VERSION=3.5.8

ENV KAFKA_VERSION=$KAFKA_VERSION \
    KAFKA_HOME=/kafka \
    ZK_VERSION=$ZK_VERSION \
    ZK_HOME=${KAFKA_HOME} \
    KAFKA_DOWNSTREAM_PATH=kafka-2.7.0

#
# Set up the kafka connect variables 
#
ENV KAFKA_CONNECT_PLUGINS_DIR=$KAFKA_HOME/connect \
    EXTERNAL_LIBS_DIR=$KAFKA_HOME/external_libs \
    CONNECT_PLUGIN_PATH=$KAFKA_CONNECT_PLUGINS_DIR \
    MAVEN_DEP_DESTINATION=$KAFKA_HOME/libs \
    CONFLUENT_VERSION=6.0.2 \
    AVRO_VERSION=1.9.2 \
    AVRO_JACKSON_VERSION=2.10.5 \
    APICURIO_VERSION=1.3.2.Final

#
# Install openjdk-11 and iproute 
#
RUN dnf -y install java-11-openjdk 
RUN dnf -y install iproute && dnf clean all

#
# Create a user and home directory for Kafka
#
USER root

RUN groupadd -r kafka -g 1001 && \
    useradd -u 1001 -r -g kafka -m -d $KAFKA_HOME -s /sbin/nologin -c "Kafka user" kafka && \
    chmod -R 755 $KAFKA_HOME

RUN mkdir $KAFKA_HOME/data && \
    mkdir $KAFKA_HOME/logs && \
    mkdir $KAFKA_HOME/txns 

#
# Set up plugins directory
#
RUN mkdir "$KAFKA_CONNECT_PLUGINS_DIR" "$EXTERNAL_LIBS_DIR"

#
# Change ownership 
#
RUN chown -R 1001 $KAFKA_HOME && \
    chgrp -R kafka $KAFKA_HOME
#
# Copy maven downloader script
#
COPY docker-maven-download.sh /usr/local/bin/docker-maven-download
RUN chmod 755 /usr/local/bin/docker-maven-download

#
# Copy Downstream Kafka
#
COPY ${KAFKA_DOWNSTREAM_PATH} ${KAFKA_HOME}

RUN mkdir $KAFKA_HOME/config.orig &&\
    mv $KAFKA_HOME/config/* $KAFKA_HOME/config.orig &&\
    chown -R 1001:kafka $KAFKA_HOME/config.orig


# Remove unnecessary files
RUN rm -f $KAFKA_HOME/libs/*-{sources,javadoc,scaladoc}.jar* 

# Install all neccesary connectors for debezium to kafka connect
#
# The `docker-entrypoint.sh` script will automatically discover the child directories
# within the $KAFKA_CONNECT_PLUGINS_DIR directory (e.g., `/kafka/connect`), and place
# all of the files in those child directories onto the Java classpath.
#
# The general recommendation is to create a separate child directory for each connector
# (e.g., "debezium-connector-mysql"), and to place that connector's JAR files
# and other resource files in that child directory.
#
# However, use a single directory for connectors when those connectors share dependencies.
# This will prevent the classes in the shared dependencies from appearing in multiple JARs
# on the classpath, which results in arcane NoSuchMethodError exceptions.
#
RUN docker-maven-download confluent kafka-connect-avro-converter "$CONFLUENT_VERSION" 4671dec77c8af4689e20419538e7b915 && \
    docker-maven-download confluent kafka-connect-avro-data "$CONFLUENT_VERSION" 5dc1111ccc4dc9c57397a2c298e6a221 && \
    docker-maven-download confluent kafka-avro-serializer "$CONFLUENT_VERSION" 5bb0c8078919e5aed55e9b59323a661e && \
    docker-maven-download confluent kafka-schema-serializer "$CONFLUENT_VERSION" 907f384780d9b75e670e6a5f4f522873 && \
    docker-maven-download confluent kafka-schema-registry-client "$CONFLUENT_VERSION" 727ef72bcc04c7a8dbf2439edf74ed38 && \
    docker-maven-download confluent common-config "$CONFLUENT_VERSION" 0cfba1fc7203305ed25bd67b29b6f094 && \
    docker-maven-download confluent common-utils "$CONFLUENT_VERSION" a940fcd0449613f956127f16cdea9935 && \
    docker-maven-download central com/fasterxml/jackson/core jackson-core "$AVRO_JACKSON_VERSION" 467e771df80da5f50fadb399f78f4ce1 && \
    docker-maven-download central com/fasterxml/jackson/core jackson-databind "$AVRO_JACKSON_VERSION" 40a3ee2381813fdcfc6ad026e914ab0c && \
    docker-maven-download central com/fasterxml/jackson/core jackson-annotations "$AVRO_JACKSON_VERSION" 2d98c7a68e9e99d98ea99dd9dc3639a4 && \
    docker-maven-download central org/apache/avro avro "$AVRO_VERSION" cb70195f70f52b27070f9359b77690bb && \
    docker-maven-download apicurio "$APICURIO_VERSION" a25a604ae3e194bc5deba0040aae2a98

ENV DEBEZIUM_VERSION="1.6.0.Beta2" \
    MAVEN_REPO_CENTRAL="" \
    MAVEN_REPOS_ADDITIONAL="" \
    MAVEN_DEP_DESTINATION=$KAFKA_CONNECT_PLUGINS_DIR \
    MONGODB_MD5=52fd5c1cd056d63c9fbb564c571bf97a \
    MYSQL_MD5=748942ecea5c416288c5f83815e06011 \
    POSTGRES_MD5=0c414ec1f0ba6e4d520eec00526df451 \
    SQLSERVER_MD5=bab3bd75a5a15da7f45df0439eea1bfb \
    ORACLE_MD5=ea7a3588f11e0c78590498463caf5920 \
    DB2_MD5=5fca335ca021602e05b4ab8d644d6af1 \
    VITESS_MD5=092ae9d579ebc27ce1cc5977b96ffaad \
    SCRIPTING_MD5=20aec1eb7932412db719b6841702417b

RUN docker-maven-download debezium mongodb "$DEBEZIUM_VERSION" "$MONGODB_MD5" && \
    docker-maven-download debezium mysql "$DEBEZIUM_VERSION" "$MYSQL_MD5" && \
    docker-maven-download debezium postgres "$DEBEZIUM_VERSION" "$POSTGRES_MD5" && \
    docker-maven-download debezium sqlserver "$DEBEZIUM_VERSION" "$SQLSERVER_MD5" && \
    docker-maven-download debezium oracle "$DEBEZIUM_VERSION" "$ORACLE_MD5" && \
    docker-maven-download debezium-additional db2 db2 "$DEBEZIUM_VERSION" "$DB2_MD5" && \
    docker-maven-download debezium-additional vitess vitess "$DEBEZIUM_VERSION" "$VITESS_MD5" && \
    docker-maven-download debezium-optional scripting "$DEBEZIUM_VERSION" "$SCRIPTING_MD5"
#
# Allow random UID to use Kafka
#
RUN chmod -R g+w,o+w $KAFKA_HOME

# Set the working directory to the Kafka home directory
WORKDIR $KAFKA_HOME

#
# Expose the ports and set up volumes for the data and logs directories
#
EXPOSE 9092 2181 2888 3888 8083
VOLUME ["/kafka/data","/kafka/logs","/kafka/config"]

#
# Copy execute scripts, entrypoint script and switch user
#
COPY ./scripts /scripts
COPY ./docker-entrypoint.sh /
RUN chmod 755 /docker-entrypoint.sh && chmod -R 755 /scripts
USER kafka
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["start"]
