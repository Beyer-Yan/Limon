#!/bin/bash
# 100GB database
# 100,500,2048,4096,8192,14336

DB_SIZE=$((100*1024*1024*1024))
CHUNKS=163840 #10GB
size=101
items=$(($DB_SIZE/$size/10000*10000))
    
echo "./kvs_bench -c nvme.json -S 4 -N $items -C $CHUNKS 2>&1 | tee result/size_${size}.txt"
../format/kvs_format -c nvme.json -D Nvme0n1 -S 32 -C 64 -N 1 
./kvs_bench -c nvme.json -S 4 -N $items -C $CHUNKS  2>&1 | tee result/size_${size}.txt
 