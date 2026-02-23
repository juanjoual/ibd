FROM hadoop-base AS base
FROM tensorflow/tensorflow:2.20.0-jupyter

RUN apt-get -qq update  \
 && DEBIAN_FRONTEND=noninteractive apt-get -qq install --no-install-recommends \
      openjdk-17-jre-headless \
      curl \
      coreutils \
 && rm -rf /var/lib/apt/lists/*

ARG USERNAME=jupyter
ARG GROUPNAME=jupyter
ARG UID=1000
ARG GID=1000

RUN groupadd -g $GID $GROUPNAME \
 && useradd -m -s /bin/bash -u $UID -g $GID $USERNAME

RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

# Hadoop
COPY --from=base --chown=$USERNAME:$GROUPNAME /opt/hadoop /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native

RUN pip install --no-cache-dir \
    hdfs \
    pandas \
    openpyxl \
    findspark \
    tensorflow-serving-api

WORKDIR /home/$USERNAME

COPY --chmod=755 run.sh /usr/local/sbin/run.sh

USER $USERNAME

CMD ["run.sh"]
