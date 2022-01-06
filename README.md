# Limon: A Persistent Key-Value Engine for Fast NVMe Storage

Limon is a high-performance persistent key-value engine built to exploit the performance potentials of the fast NVMe storage. Limon considers four practical design requirements that existing KV engines have not fully satisfied: functionality, compactness, scalability and stability. Limon proposes four novel designs:

- the semi-shared architecture with globally shared in-memory index and partitioned on-disk data.
- the efficient user-space Salloc to manage storage space with light-weight defragmentation.
- the mapping table to decouple index and KV pairs to reduce synchronization overhead for shared index. 
- the Dasio over SPDK to deliver efficient per-core asynchronous I/O processing.

# Install

Limon is written in C (C11) and relies on SPDK runtime. Install SPDK from https://github.com/spdk/spdk and follow the internal installation manual (tested in v21.10).

Clone Limon into the same parent directory with SPDK.

> git clone https://github.com/Beyer-Yan/Limon
> make

The `make` command will build limon into the static library named liblimon.a, which locates in spdk/build/lib. Nextly, build the application.

> cd Limon/apps/format
> make

This will generate the `mkfs.limon` command, which is responsible for formatting the low-level block device.

> cd Limon/apps/benchmark
> make

This will build the benchmark tool, i.e., YCSB and ETC, and generate the `kvs_bench` command.

# Run Benchmarks

Ensure the low-level block device has been formatted by `mkfs.limon`. Limon is formatted on spdk block device, which can be a user-space NVMe driver exporting the raw PCIe address or the kernel bdev with uring or aio accessing interface. 

Note that spdk requires huge-page memory, 

> echo 1024 > xxxx/nr_huagepages

This will allocate 2GB huge pages.

## Format the Bdev

Edit the json file needed by spdk bdev module. For example, if you choose the user-space driver of spdk, the json should be specified with the following format.

```
{  
    "subsystems": [  
      {  
        "subsystem": "bdev",  
        "config": [  
          {  
            "method": "bdev_nvme_attach_controller",  
            "params": {  
                "trtype": "PCIe",  
                "name": "Nvme2",  
                "traddr": "0000:86:00.0"
            }  
          }  
        ]  
      }
    ]  
  }
```
The `name` the `traddr` are configured according to your platform.

If you use the kernel bdev with the io_uring access interface, then specify the json file with the following format.

```
{
    "subsystems": [
      {
        "subsystem": "bdev",
        "config": [
          {
            "method": "bdev_uring_create",
            "params": {
                "name": "Nvme2",
                "filename": "/dev/nvme2n1"
            }
          }
        ]
      }
    ]
  }
```
The `name` and the `filename` are configured according to your platform. Note that io_uring requires the uring building parameter in spdk, see the `configure --help` in spdk build process.

Nextly, run the `mkfs.limon` with your parameter to format the bdev.

For example,
> mkfs.limon -c nvme.json -S 32 -C 4 -N 1 -D Nvme2n1 -f

It formats the block device named Nvme2n1 with 32 partitions, 4MB reclaim node size, 1 default reclaim node for each sized class. The command with -E dumps the current storage layout.

> mkfs.limon -c nvme.json -D Nvme2n1 -E

## Run the Benchmark

After the block device is formatted, run `kvs_bench` with `-P` to generate test database.

For example,
> cd Limon/apps/benchmark
> ./kvs_bench -c nvme.json -I 2 -P

Then run `kvs_bench` without `-P` to start the benchmark.

> cd Limon/apps/benchmark
> ./kvs_bench -c nvme.json -S 4 -I 2.

The above command starts the benchmark with 4 workers and 2 clients.

## Run with Device Aggregation

The spdk bdev module originally supports the RAID0 device aggregation. If your test platform is equipped with multiple block devices, these devices can be aggregated as a RAID0 bdev. Specify the json file as follows

```
{  
    "subsystems": [  
      {  
        "subsystem": "bdev",  
        "config": [  
          {  
            "method": "bdev_nvme_attach_controller",  
            "params": {  
                "trtype": "PCIe",  
                "name": "Nvme2",  
                "traddr": "0000:86:00.0"
            }  
          }  
        ]  
      }
    ]  
  }
```

Then run `mkfs` and `kvs_bench` with the device named `RAIDdev` to enable the device aggregation.

# Define Your Benchmark

Currently, Limon is started as a research project, so the benchmark parameters are hard-coded only for YCSB and ETC workloads to reduce the engineering efforts. If you want to specify your test parameters, just modify the code.

For example of the following code lines in `apps/benchmark/kvs_bench.c:175-180`

```C
struct workload w = {
    .api = &ETC,
    .nb_items_in_db = _g_default_opts.nb_items,
    .nb_load_injectors = _g_default_opts.nb_injectors,
    .start_core = 20,
};
```

The `start_core = 20` means that the clients will start from CPU core id=20.

Change the `.api=&YCSB` to enable the YCSB benchmark, and specify the workloads for YCSB in code lines `apps/benchmark/kvs_bench.c:192-215`

```C
bench_t workloads[] = {
    //ycsb_f_uniform
    //ycsb_a_uniform,ycsb_e_uniform
    //ycsb_a_zipfian,
    //ycsb_a_uniform
    //ycsb_c_uniform,
    //ycsb_e_uniform
    //ycsb_c_zipfian,
    //ycsb_c_zipfian
    //ycsb_a_uniform,ycsb_c_uniform,ycsb_e_uniform,
    //ycsb_a_zipfian,ycsb_c_zipfian,ycsb_e_zipfian,
};
```

Note that, in YCSB, the KV size is hard-coded with 1KB. If you want to change the test size, you can change the code line `apps/benchmark/ycsb/ycsb.c:11`

> size_t item_size = 1024;

In ETC workload, the KV sizes are dynamically generated.

## Implement Your Global Index

Currently, the global index in Limon is implemented as the concurrent ART with epoch based garbage collection by C++ (the dynamic library in Limon/lib/libcart.so). It is well known that the ART is a trie based index that is not very friendly to scan-intensive workloads. You can implement your own global index, i.e., the concurrent B+tree, with the following C interface, seen in `include/index.h`.

```C
struct mem_index * mem_index_init(void);
void mem_index_destroy(struct mem_index* mem_index);
int mem_index_add(struct mem_index *mem_index, struct kv_item *item, uint64_t sid);
void mem_index_delete(struct mem_index *mem_index,struct kv_item *item);
uint64_t mem_index_lookup(struct mem_index *mem_index, struct kv_item *item);
void mem_index_scan(struct mem_index *mem_index,struct kv_item *item, int maxLen, int* founds, uint64_t *sid_array);
```

# Notes

All experiments are performed in ubuntu xxx with linux kernel version xxx, spdk xxx and gcc xxx.
