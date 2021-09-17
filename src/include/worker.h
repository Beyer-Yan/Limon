#ifndef KVS_WORKER_H
#define KVS_WORKER_H
#include "item.h"
#include "slab.h"
#include "index.h"
#include "spdk/blob.h"
#include "spdk/thread.h"

struct worker_context;
struct meta_worker_context;

struct worker_init_opts{
    uint32_t max_request_queue_size_per_worker;
    //uint32_t max_io_pending_queue_size_per_worker;
    uint32_t reclaim_batch_size;
    uint32_t reclaim_percentage_threshold;
    uint32_t io_cycle; //us

    uint64_t nb_init_pages;
    struct mem_index* global_index;

    uint32_t nb_shards;
    struct slab_shard *shard;

    struct spdk_blob_store *target;

    struct spdk_thread *meta_thread;

    //average chunks for each worker.
    uint64_t chunk_cache_water_mark;

    uint32_t core_id;
    
    uint32_t nb_reclaim_shards;
    uint32_t reclaim_shard_start_id;
};

struct worker_context* worker_alloc(struct worker_init_opts* opts);
void worker_start(struct worker_context* wctx);
void worker_destroy(struct worker_context* wctx);

/**
 * @brief When in recovering state, the work is not ready.
 */
bool worker_is_ready(struct worker_context* wctx);

//The item in kv_cb function is allocated temporarily. If you want to do something else, please copy it out 
//in kv_cb function

typedef void (*kv_cb)(void* ctx, struct kv_item* item, int kverrno);

/**
 * @brief      Process the GET request for the worker
 * 
 * @param wctx   The worker context
 * @param item   The item to get.
 * @param sid    The sid number of the given key
 * @param cb_fn  The user callback
 * @param ctx    The callback context
 */
void worker_enqueue_get(struct worker_context* wctx,struct kv_item *item,uint64_t sid, kv_cb cb_fn, void* ctx);

/**
 * @brief        Process the PUT request for the worker
 * 
 * @param wctx   The worker context
 * @param shard  The shard id of the given item
 * @param item   The item to put. Ignored if the sid is valid.
 * @param sid    The sid. 0 means add.
 * @param cb_fn  The user callback
 * @param ctx    The callback context
 */
void worker_enqueue_put(struct worker_context* wctx,uint32_t shard,struct kv_item *item,uint64_t sid, kv_cb cb_fn, void* ctx);

/**
 * @brief        Delete the given item
 * 
 * @param wctx   The worker context
 * @param sid    The valid sid
 * @param cb_fn  The user callback
 * @param ctx    The callback context
 */
void worker_enqueue_delete(struct worker_context* wctx, struct kv_item *item, uint64_t sid, kv_cb cb_fn, void* ctx);

struct worker_statistics{
    uint64_t chunk_hit_times;
    uint64_t chunk_miss_times;
    uint32_t nb_pending_reqs;
    uint32_t nb_pending_ios;
    uint64_t nb_used_pages;
};
void worker_get_statistics(struct worker_context* wctx, struct worker_statistics* ws_out);

struct meta_init_opts{
    uint32_t nb_business_workers;
    uint32_t core_id;
    struct spdk_thread* meta_thread;
    struct worker_context **wctx_array;
};
struct meta_worker_context* meta_worker_alloc(struct meta_init_opts *opts);
void meta_worker_start(struct meta_worker_context* meta);
void meta_worker_destroy(struct meta_worker_context* meta);

#endif
