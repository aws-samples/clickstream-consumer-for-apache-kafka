FROM centos:latest

RUN cd /etc/yum.repos.d/
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*

RUN yum install -y wget tar openssh-server openssh-clients sysstat sudo which openssl hostname
RUN yum install -y java-1.8.0-openjdk-devel
RUN yum install -y epel-release &&\
  yum install -y jq &&\
  yum install -y nmap-ncat git
ARG MAVEN_VERSION=3.5.4

# Maven
RUN curl -fsSL https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar xzf - -C /usr/share \
  && mv /usr/share/apache-maven-$MAVEN_VERSION /usr/share/maven \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_VERSION=${MAVEN_VERSION}
ENV M2_HOME /usr/share/maven
ENV maven.home $M2_HOME
ENV M2 $M2_HOME/bin
ENV PATH $M2:$PATH

# Prometheus Java agent
RUN wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.13.0/jmx_prometheus_javaagent-0.13.0.jar
RUN mv jmx_prometheus_javaagent-0.13.0.jar /opt

RUN git clone https://github.com/aws-samples/sasl-scram-secrets-manager-client-for-msk.git
WORKDIR sasl-scram-secrets-manager-client-for-msk
RUN mvn clean install -f pom.xml
WORKDIR ../

RUN mkdir clickstream-consumer-for-apache-kafka
WORKDIR clickstream-consumer-for-apache-kafka
COPY src/ ./src/
COPY pom.xml ./
RUN mvn clean install -f pom.xml
COPY start-kafka-consumer.sh consumer.properties kafka-producer-consumer.yml ./
RUN chmod 777 start-kafka-consumer.sh
# cleanup
RUN yum clean all;

EXPOSE 3801
ENTRYPOINT ./start-kafka-consumer.sh
