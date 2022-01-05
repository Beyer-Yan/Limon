#!/bin/bash

DB_SIZE=$((50*1024*1024*1024)) #50GB
CHUNKS=81920 #5GB
size=1024
items=$(($DB_SIZE/$size/10000*10000))

for i in {0,50,100,150,200,250,300}
do
    echo "./kvs_bench -c nvme.json -N ${items} -T ${i} 2>&1 | tee result/cycle_${size}_${i}.txt"
    ../format/kvs_format -c nvme.json -D Nvme0n1 -S 32 -C 64 -N 1 
    ./kvs_bench -c nvme.json -N ${items} -T ${i} 2>&1 | tee result/cycle_${size}_${i}.txt
done