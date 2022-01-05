#ifndef KVS_H
#define KVS_H
#include <stdbool.h>
#include "index.h"
#include "item.h"
#include "worker.h"

#include "spdk/event.h"

struct kvs_start_opts{
    char* kvs_name;
    uint32_t nb_works;
    uint32_t max_request_queue_size_per_worker;
    uint32_t max_cache_pages;

    uint32_t reclaim_batch_size;
    uint32_t reclaim_percentage_threshold;
    char*    devname;

    uint32_t io_cycle; //us

    struct spdk_app_opts *spdk_opts;
    void (*startup_fn)(void*ctx, int kverrno);
    void* startup_ctx;
};

//The kvs is implemented in sinleton mode. Only one instance is the kvs allowed to startup.
void kvs_start_loop(struct kvs_start_opts *opts);
void kvs_shutdown(void);
bool kvs_is_started(void);

// The key filed of item  shall be filed
void kv_get_async(struct kv_item *item, kv_cb cb_fn, void* ctx);

//Get with no item.
void kv_get_with_sid_async(uint64_t sid, kv_cb cb_fn, void* ctx);

// The whole item shall be filled
void kv_put_async(struct kv_item *item, kv_cb cb_fn, void* ctx);

//The interface for benchmark
void kv_populate_async(struct kv_item *item, kv_cb cb_fn, void* ctx);

// The key field of item shall be filled
void kv_delete_async(struct kv_item *item, kv_cb cb_fn, void* ctx);

void kv_reset_io_stats(void);

void kv_print_io_stats(void);

/**
 * @brief  Scan from the global index sychronuously
 * 
 * @param item        The start item
 * @param maxLen      The max scan length
 * @param sid_array   The result array of sid
 * @return uint64_t   The number actual found items;
 */
uint64_t kv_scan(struct kv_item *item, uint64_t maxLen,uint64_t* sid_array);

struct slab_statistics{
    uint64_t nb_shards;
    uint64_t nb_slabs_per_shard;
    struct {
        uint64_t slab_size;
        uint64_t nb_slots;
        uint64_t nb_free_slots;
    }slabs[0];
};

struct kvs_runtime_statistics{
    uint64_t nb_worker;
    struct worker_statistics ws[0];
};

struct slab_statistics* kvs_get_slab_statistcs(void);
struct kvs_runtime_statistics* kvs_get_runtime_statistics(void);
uint64_t kvs_get_nb_items(void);

#endif
