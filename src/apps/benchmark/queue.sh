#!/bin/bash
for i in {1,2,4,8,16,32,64}
do
    echo "./kvs_bench -c nvme.json -Q ${i} 2>&1 | tee result/q_${i}.txt"
    ./kvs_bench -c nvme.json -Q ${i} 2>&1 | tee result/q_${i}.txt
done