FROM hadoop-base AS base
FROM quay.io/jupyter/pyspark-notebook

USER root

# Hadoop
COPY --from=base --chown=${NB_UID}:${NB_GID} /opt/hadoop /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$HADOOP_HOME/sbin:$HADOOP_HOME/bin:$PATH
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native

RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

RUN pip install --no-cache-dir hdfs

WORKDIR /home/${NB_USER}

COPY --chmod=755 run.sh /usr/local/sbin/run.sh

USER ${NB_UID}

CMD ["run.sh"]
