#ifndef KVS_H
#define KVS_H
#include <stdbool.h>
#include "index.h"
#include "item.h"
#include "worker.h"

#include "spdk/event.h"

struct kvs_start_opts{
    // nb_worker should be 1,2,4,8,16... and less than nb_shards
    char* kvs_name;
    uint32_t nb_works;
    uint32_t max_request_queue_size_per_worker;
    uint32_t max_io_pending_queue_size_per_worker;
    uint32_t max_cache_chunks;

    uint32_t reclaim_batch_size;
    uint32_t reclaim_percentage_threshold;
    char* devname;

    struct spdk_app_opts *spdk_opts;
};

//The kvs is implemented in sinleton mode. Only one instance is the kvs allowed to startup.
void kvs_start_loop(struct kvs_start_opts *opts);
bool kvs_is_started(void);

typedef void (*kv_cb)(void* ctx, struct kv_item* item, int kverrno);

// The key filed of item  shall be filed
void kv_get_async(const struct kv_item *item, kv_cb cb_fn, void* ctx);

// The whole item shall be filled
void kv_put_async(const struct kv_item *item, kv_cb cb_fn, void* ctx);

// The key field of item shall be filed
void kv_delete_async(const struct kv_item *item, kv_cb cb_fn, void* ctx);

struct kv_iterator;

//The seek and next and kv_iterator_item operations iterate only the key of a item. If you want to load the
//whole item data, you can issue a kv_get_async command.
//seek to first element.
//All scan interfaces are designed as sync interfaces, as any one of the scan operations 
//will not issue disk IO. They just performing lookuping in in-mem index.
struct kv_iterator* kv_iterator_alloc(void);
void kv_iterator_release(struct kv_iterator *it);
bool kv_iterator_first(struct kv_iterator *it);
bool kv_iterator_seek(struct kv_iterator *it, struct kv_item *item);
bool kv_iterator_next(struct kv_iterator *it);
struct kv_item* kv_iterator_item(struct kv_iterator *it);

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
