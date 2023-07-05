FROM couchbase/server:community-7.1.0-aarch64

ENV CLUSTER_NAME="Cluster"
ENV USERNAME="Administrator"
ENV PASSWORD="password"
ENV BUCKET_NAME="sample"

ADD configure.sh /configure.sh
RUN chmod +x /configure.sh
RUN echo " image starting"

EXPOSE 8091 8092 8093 8094 8095 8096 11207 11210 11211

CMD ["/configure.sh"]
