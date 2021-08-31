#!/bin/bash
for i in {1,2,4,8,16}
do
    echo "./kvs_bench -c nvme.json -S ${i} 2>&1 | tee result/workers_${i}.txt"
    ./kvs_bench -c nvme.json -S ${i} 2>&1 | tee result/workers_${i}.txt
done