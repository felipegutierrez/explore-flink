#!/bin/sh

# remove "tpch-dbgen" if exists and download
git clone https://github.com/electrum/tpch-dbgen.git tpch-dbgen-tmp

cp -R tpch-dbgen-tmp/* tpch-dbgen/
# rm -Rf tpch-dbgen-tmp

# replace variables
cd tpch-dbgen
sed -i '/CC      =/c\CC=gcc' makefile.suite
sed -i '/DATABASE=/c\DATABASE=INFORMIX' makefile.suite
sed -i '/MACHINE =/c\MACHINE=LINUX' makefile.suite
sed -i '/WORKLOAD =/c\WORKLOAD=TPCH' makefile.suite

# compile
make -f makefile.suite

# run the tpch-dbgen to generate data with database factor of 0.1GB
mkdir -p data && cd data
cp ../dbgen . && cp ../dists.dss .
./dbgen -f -s 0.1
ls -l

# create the datarate.txt file
mkdir -p /tmp
echo "1000000000" > /tmp/datarate.txt

# keeps the container up and running
sleep infinity & wait
