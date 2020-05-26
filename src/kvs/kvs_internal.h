#ifndef KVS_INTERNAL_H
#define KVS_INTERNAL_H
#include "kvs.h"
#include "slab.h"
#include "worker.h"
#include "kvutil.h"
#include "kverrno.h"

#include "spdk/blob.h"

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

    //For any operation for blob, it shall be send to meta thread
    struct spdk_blob *super_blob;
    struct spdk_blob_store *bs_target;
    struct spdk_io_channel *meta_channel;
    struct spdk_thread *meta_thread;

    struct chunkmgr_worker_context *chunkmgr_worker;
    uint32_t nb_workers;
    struct worker_context **workers;
};

extern struct kvs *g_kvs;

void kvs_shutdown(void);

#endif
