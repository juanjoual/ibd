FROM hadoop-base

ENV DATADIR=/opt/hadoop/dfs/data
RUN mkdir -p /opt/hadoop/dfs/data

COPY --chmod=755 run.sh /usr/local/sbin/run.sh
CMD ["run.sh"]
