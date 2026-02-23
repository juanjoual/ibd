FROM ubuntu:24.04

ARG USERNAME=ubuntu
ARG GROUPNAME=ubuntu

RUN apt-get -qq update  \
     && DEBIAN_FRONTEND=noninteractive apt-get -qq install --no-install-recommends \
     openjdk-17-jre-headless \
     curl \
     gnupg \
     procps \
     python3 \
     coreutils \
     && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 2

RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# Hadoop, usamos un CDN local para que descargue más rápido.
ARG HADOOP_VERSION=3.4.2
#ARG HADOOP_URL=https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
ARG HADOOP_URL=http://192.168.70.194/hadoop-$HADOOP_VERSION.tar.gz
ENV HADOOP_HOME=/opt/hadoop

RUN set -x \
     && curl -fsSL https://archive.apache.org/dist/hadoop/common/KEYS -o /tmp/hadoop-KEYS \
     && gpg --import /tmp/hadoop-KEYS \
     && mkdir $HADOOP_HOME \
     && curl -fsSL $HADOOP_URL -o /tmp/hadoop.tar.gz \
     && curl -fsSL $HADOOP_URL.asc -o /tmp/hadoop.tar.gz.asc \
     && gpg --verify /tmp/hadoop.tar.gz.asc \
     && tar -xf /tmp/hadoop.tar.gz -C $HADOOP_HOME --strip-components 1 \
     && mkdir $HADOOP_HOME/logs \
     && chown -R $USERNAME:$GROUPNAME $HADOOP_HOME \
     && rm /tmp/hadoop*

ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native

COPY --chown=$USERNAME:$GROUPNAME conf/ $HADOOP_CONF_DIR/
COPY --chmod=755 entrypoint.sh /usr/local/sbin/entrypoint.sh

USER $USERNAME

ENTRYPOINT ["entrypoint.sh"]
