# Limon: A Persistent Key-Value Engine for Fast NVMe Storage

Limon is a high-performance persistent key-value engine built to exploit the performance potentials of the fast NVMe storage. Limon considers four practical design requirements that existing KV engines have not fully satisfied: functionality, compactness, scalability and stability. Limon proposes four novel designs:

- the semi-shared architecture with globally shared in-memory index and partitioned on-disk data.
- the efficient user-space record layout without logging and frequent garbage collections to manage storage space.
- the optimized I/O processing with SPDK to deliver efficient per-core asynchronous I/O processing.

# Install

Limon is written in C (C11) and relies on SPDK runtime. Install SPDK from https://github.com/spdk/spdk and follow the internal installation manual (tested in v21.07).

Clone Limon into the same parent directory with SPDK.

```shell
git clone https://github.com/Beyer-Yan/Limon
make
```

The `make` command will build limon into the static library named libspdk_limon.a, which locates in spdk/build/lib. Nextly, build the application.

```shell
cd Limon/apps/format
make
```

This will generate the `mkfs.limon` command, which is responsible for formatting the low-level block device.

```shell
cd Limon/apps/benchmark
make
```

This will build the benchmark tool, i.e., YCSB and ETC, and generate the `kvs_bench` command.

# Run Benchmarks

Ensure the low-level block device has been formatted by `mkfs.limon`. Limon is formatted on spdk block device, which can be a user-space NVMe driver exporting the raw PCIe address or the kernel bdev with uring or aio accessing interface. 

Note that spdk requires huge-page memory, 

```shell
echo 1024 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
```

This will allocate 2GB huge pages.

## Format the Bdev

Edit the json file needed by spdk bdev module. For example, if you choose the user-space driver of spdk, the json should be specified with the following format.

```json
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

```json
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

```
./mkfs.limon --help
 ...
 -K, --max-key-length <num>   the max key length of the current kvs(default:256) 
 -S, --shards <num>           the number of shards(default:4)
 -C, --chunks-per-node <num>  the chunks per reclaim node(default:4)
 -f, --force-format           format the kvs forcely
 -D, --devname <namestr>      the devname(default:Nvme2n1)
 -N, --init-nodes  <num>      the init nodes for each slab(default:16)
 -E, --dump                   Dump the existing kvs format
```

For example,

```shell
mkfs.limon -c nvme.json -S 32 -C 4 -N 1 -D Nvme2n1 -f
```

It formats the block device named Nvme2n1 with 32 partitions, 4MB reclaim node size, 1 default reclaim node for each sized class. The command with -E dumps the current storage layout.

```shell
mkfs.limon -c nvme.json -D Nvme2n1 -E
```

## Run the Benchmark

After the block device is formatted, run `kvs_bench` with `-P` to generate test database.

```shell
./kvs_bench --help
 ...
 -D, --devname <namestr>      block devname(default:Nvme2n1)
 -S, --workers <num>          number of workers(default:1)
 -I, --injectors <num>        number of injector(default:4)
 -Q, --queue-size <num>       queue size(default:16)
 -C, --caches <numGB>         number of cache chunks(deprecated)
 -N, --items  <num>           total items in db(default:50000000)
 -T, --io-cycle_us <num>      io polling cycle(default:0)
 -P, --populate_db            load database (default:run benchmark)
```

For example,

```shell
cd Limon/apps/benchmark
./kvs_bench -c nvme.json -I 2 -P
```

Then run `kvs_bench` without `-P` to start the benchmark.

```shell
cd Limon/apps/benchmark
./kvs_bench -c nvme.json -S 4 -I 2
```

The above command starts the benchmark with 4 workers and 2 clients.

## Run with Device Aggregation

The spdk bdev module originally supports the RAID0 device aggregation. If your test platform is equipped with multiple block devices, these devices can be aggregated as a RAID0 bdev. Seen RAID at https://spdk.io/doc/jsonrpc.html.

Then run `mkfs` and `kvs_bench` with the RAID0 block device to enable the device aggregation.

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

All experiments passed through in ubuntu 20.04 with linux kernel version 5.11.0, spdk 21.07 and gcc 9.3.0.
