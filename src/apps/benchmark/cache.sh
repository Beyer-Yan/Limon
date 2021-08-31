#!/bin/bash
CHUNKS_BASE=81920 # 5GB

for i in {2,4,6,8}
do
    chunks=$((${i}*${CHUNKS_BASE}))
    if [ $chunks -eq 0 ]; then
        chunks=16384 # 1GB
    fi
    # do test
    echo "./kvs_bench -c nvme.json -C $chunks 2>&1 | tee result/cache_${i}.txt"
    ./kvs_bench -c nvme.json -C $chunks 2>&1 | tee result/cache_${i}.txt
done
