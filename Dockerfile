FROM golang

RUN apt-get update && \
    apt-get install -y --no-install-recommends iproute2 iputils-ping && \
    rm -rf /var/lib/apt/lists/*

ENV NAME=/swiftpaxos

COPY bin $NAME/bin/
COPY base.conf swiftpaxos-version $NAME/

WORKDIR $NAME

ENV CONFIG base.conf
ENV TYPE master
ENV ADDR 127.0.0.1
ENV MADDR 127.0.0.1
ENV ALIAS localhost
ENV NSERVERS 1

ENTRYPOINT ["bin/run.sh"]
