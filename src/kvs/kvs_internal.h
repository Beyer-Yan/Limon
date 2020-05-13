#include "slab.h"
#include "worker.h"
#include "kvutil.h"
#include "kverrno.h"

struct kvs{

    char* kvs_name;

    uint32_t max_key_length;
    uint32_t max_cache_chunks;

    uint32_t max_request_queue_size_per_worker;
    uint32_t max_io_pending_queue_size_per_worker;

    uint32_t reclaim_batch_size;
    uint32_t reclaim_percentage_threshold;

    uint32_t nb_shards;
    struct slab_shard *shards;

    struct chunkmkgr_worker_context *chunkmgr_worker;
    uint32_t nb_workers;
    struct worker_context **workers;

};

extern volatile struct kvs *g_kvs;