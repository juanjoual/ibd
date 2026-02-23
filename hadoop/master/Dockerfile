FROM hadoop-base

ENV NAMEDIR=/opt/hadoop/dfs/name
RUN mkdir -p /opt/hadoop/dfs/name

COPY --chmod=755 run.sh /usr/local/sbin/run.sh
CMD ["run.sh"]
