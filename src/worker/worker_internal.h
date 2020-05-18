#ifndef KVS_WORKER_INTERNAL_H
#define KVS_WORKER_INTERNAL_H

#include <stddef.h>
#include <stdatomic.h>
#include "worker.h"
#include "slab.h"
#include "uthash.h"
#include "reclaim.h"
#include "pool.h"
#include "iomgr.h"
#include "pagechunk.h"
#include "kvutil.h"
#include "spdk/thread.h"

enum op_code { GET=0, PUT, DELETE, FIRST, SEEK, NEXT,};

struct process_ctx{
    struct worker_context *wctx;
    struct index_entry* entry;
    
    //a cached slab pointer to prevent the repetive computing for slab.
    struct slab* slab;
    //To record the old slab in case of the change of an item size.
    struct slab* old_slab;
    
    //When an item is updated not in place, this field will be used to record
    //the newly allocated entry infomation.
    struct index_entry new_entry;

    //For resubmit request, it is unnecessary to re-lookup in the mem index.
    uint32_t no_lookup;
};

/**
 * @brief kv_request is used for order guaranteeing. A request is, firstly, put into kv_request
 * queue. Each worker gets a kv_request object every time and put it into kv_request_internal 
 * queue.
 * Requests from the kv_request queue are processed oerderly. But requests from kv_request_internal
 * queue are processed out-of-order.
 * 
 */
struct kv_request{
    uint32_t op_code;
    uint32_t shard;
    const struct kv_item* item;
    kv_cb cb_fn;
    void* ctx;
} __attribute__(( aligned(CACHE_LINE_LENGTH) ));

struct kv_request_internal{
    TAILQ_ENTRY(kv_request_internal) link;

    uint32_t op_code;
    uint32_t shard;
    const struct kv_item* item;
    kv_cb cb_fn;
    void* ctx;

    struct process_ctx pctx;
};


struct worker_context{
    
    struct mem_index *mem_index;
    
    //The shards filed points to the shards field of kvs structure.
    struct slab_shard *shards;
    uint32_t nb_shards;

    //The migrating of shards below are processed by this worker. 
    uint32_t nb_reclaim_shards;
    uint32_t reclaim_shards_start_id;
    uint32_t reclaim_percentage_threshold;

    //simple thread safe mp-sc queue
    struct kv_request *request_queue;  // lock-free queue, multi-prod-single-cons
    atomic_uint buffered_request_idx;  // Number of requests enqueued or in the process of being enqueued         
    atomic_uint sent_requests;         // Number of requests fully enqueued     
    atomic_uint processed_requests;    // Number of requests fully submitted and processed on disk  

    uint32_t max_pending_kv_request;          // Maximum number of enqueued requests 

    // thread unsafe queue, but we do not care the unsafety in single thread program
    TAILQ_HEAD(, kv_request_internal) submit_queue;
    TAILQ_HEAD(, kv_request_internal) resubmit_queue;

    struct object_cache_pool *kv_request_internal_pool;

    struct pagechunk_mgr *pmgr;
    struct reclaim_mgr   *rmgr;
    struct iomgr         *imgr;

    struct spdk_thread* thread;
    struct spdk_blob_store *target;
    
} __attribute__(( aligned(CACHE_LINE_LENGTH) ));

void worker_process_get(struct kv_request_internal *req);
void worker_process_put(struct kv_request_internal *req);
void worker_process_delete(struct kv_request_internal *req);

//For range scan operation
void worker_process_first(struct kv_request_internal *req);
void worker_process_seek(struct kv_request_internal *req);
void worker_process_next(struct kv_request_internal *req);

/*----------------------------------------------------*/
//Reclaim related

typedef void (*reclaim_io_cb)(void* ctx, int kverrno);

struct reclaim_ctx{
    struct worker_context* wctx;
    struct chunk_desc *desc;
    uint32_t no_lookup;
};

struct pending_item_delete{
    uint64_t slot_idx;
    struct slab *slab;

    reclaim_io_cb io_cb_fn;
    void *ctx;

    struct reclaim_ctx rctx;
    TAILQ_ENTRY(pending_item_delete) link;
};

struct pending_item_migrate{
    uint64_t slot_idx;
    struct slab *slab;
    struct index_entry* entry;

    //Variables below will be used when I allocate a new slot.
    uint64_t new_slot;
    struct chunk_desc* new_desc;

    reclaim_io_cb io_cb_fn;
    void *ctx;

    struct reclaim_ctx rctx;
    TAILQ_ENTRY(pending_item_migrate) link;
};

//Each pending_slab_migrate operations includes multi pending_item_migrate operations.
struct slab_migrate_request{
    struct slab* slab;
    struct reclaim_node* node;
    struct worker_context *wctx;

    //If a fault hits, I should abort the migrating.
    bool is_fault;

    uint64_t nb_processed;
    uint64_t start_slot;
    uint64_t cur_slot;
    uint64_t last_slot;

    TAILQ_ENTRY(slab_migrate_request) link;
};

struct reclaim_mgr{
    uint32_t migrating_batch;
    uint32_t reclaim_percentage_threshold;
    /**
     * @brief Construct a new tailq head object.
     * Each background item deleting will be appended to the list. The reclaim thread
     * poll will process these pending deleting.
     */
    TAILQ_HEAD(,pending_item_delete) item_delete_head;

    /**
     * @brief Construct a new tailq head object.
     * When a slab needs shrinking, each item to be shrunk is appended to the list.
     * When an item fails to be migrated because of puting or scanning or deleting, 
     * it will be appended to the list and is resubmited in the next migrating cycle.
     */
    TAILQ_HEAD(,pending_item_migrate) item_migrate_head;
    TAILQ_HEAD(,pending_item_migrate) remigrating_head;

    /**
     * @brief Construct a new tailq head object.
     * If a slab needs migrating, it will issue a migrating request for the last
     * reclaim node. 
     * 
     */
    TAILQ_HEAD(,slab_migrate_request) slab_migrate_head;

    struct object_cache_pool *pending_delete_pool;
    struct object_cache_pool *pending_migrate_pool;
    struct object_cache_pool *migrate_slab_pool;
};

extern void worker_reclaim_post_deleting(struct reclaim_mgr* rmgr,
                          struct slab* slab, 
                          uint64_t slot_idx,
                          void (*cb)(void* ctx, int kverrno),
                          void* ctx);

int worker_reclaim_process_pending_item_delete(struct worker_context *wctx);
int worker_reclaim_process_pending_item_migrate(struct worker_context *wctx);
int worker_reclaim_process_pending_slab_migrate(struct worker_context *wctx);

/*----------------------------------------------------*/
//Recovery related
//
void worker_perform_rebuild_async(struct worker_context *wctx, 
                                  void(*complete_cb)(void*ctx, int kverrno),void*ctx);

#endif
