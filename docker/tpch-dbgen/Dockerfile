FROM gcc AS builder

RUN mkdir -p /opt
COPY ./generate-tpch-dbgen.sh /opt/generate-tpch-dbgen.sh
WORKDIR /opt
RUN chmod +x generate-tpch-dbgen.sh && ./generate-tpch-dbgen.sh
