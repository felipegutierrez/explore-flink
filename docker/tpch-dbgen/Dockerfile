FROM gcc AS builder

RUN mkdir -p /opt
COPY ./generate-tpch-dbgen.sh /opt/generate-tpch-dbgen.sh
WORKDIR /opt
RUN chmod +x /opt/generate-tpch-dbgen.sh
ENTRYPOINT ["/bin/sh","/opt/generate-tpch-dbgen.sh"]
