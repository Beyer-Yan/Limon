#ifndef KVS_WORKER_INTERNAL_H
#define KVS_WORKER_INTERNAL_H

#include <stddef.h>
#include <stdatomic.h>
#include "index.h"
#include "worker.h"
#include "slab.h"
#include "uthash.h"
#include "pool.h"
#include "io.h"
#include "pagechunk.h"
#include "kvutil.h"

#include "spdk/thread.h"
#include "spdk/env.h"

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
 * Requests from the kv_request queue are processed orderly. But requests from kv_request_internal
 * queue are processed out-of-order.
 * 
 */
struct kv_request{
    uint32_t op_code;
    uint32_t shard;
    struct kv_item* item;
    worker_cb cb_fn;
    void* ctx;
} __attribute__(( aligned(CACHE_LINE_LENGTH) ));

struct kv_request_internal{
    TAILQ_ENTRY(kv_request_internal) link;

    uint32_t op_code;
    uint32_t shard;
    struct kv_item* item;
    worker_cb cb_fn;
    void* ctx;

    struct process_ctx pctx;
};


struct worker_context{
    volatile bool ready;
    struct mem_index *mem_index;
    
    //The shards filed points to the shards field of kvs structure.
    struct slab_shard *shards;
    uint32_t nb_shards;
    uint32_t core_id;

    //The migrating of shards below are processed by this worker. 
    uint32_t nb_reclaim_shards;
    uint32_t reclaim_shards_start_id;
    uint32_t reclaim_percentage_threshold;

    //simple thread safe mp-sc queue
    struct kv_request *request_queue;  // kv_request mempool for this worker
    uint32_t max_pending_kv_request;   // Maximum number of enqueued requests 

    // thread unsafe queue, but we do not care the unsafety in single thread program
    TAILQ_HEAD(, kv_request_internal) submit_queue;
    TAILQ_HEAD(, kv_request_internal) resubmit_queue;

    //User thread get a free kv_req and enqueue it to req_used_ring.
    //worker thread get a used kv_req and enqueu a free req to req_free_ring.
    struct spdk_ring *req_used_ring;  
    struct spdk_ring *req_free_ring;
    
    struct spdk_poller *business_poller;
    struct spdk_poller* slab_evaluation_poller;
    struct spdk_poller* reclaim_poller;

    struct object_cache_pool *kv_request_internal_pool;

    struct pagechunk_mgr *pmgr;
    struct reclaim_mgr   *rmgr;
    struct iomgr         *imgr;

    struct spdk_thread* thread;
    
} __attribute__(( aligned(CACHE_LINE_LENGTH) ));

void worker_process_get(struct kv_request_internal *req);
void worker_process_put(struct kv_request_internal *req);
void worker_process_delete(struct kv_request_internal *req);

//For range scan operation
void worker_process_first(struct kv_request_internal *req);
void worker_process_seek(struct kv_request_internal *req);
void worker_process_next(struct kv_request_internal *req);


/*----------------------------------------------------*/
//page chunk related
/**
 * @brief The page chunk manager thead is a special thread to manager all chunk memory。
 * There is only one page chunk manager thead in the system. It is recommended to spawn
 * the thread on a special cpu core in case that the chunk request is not process in
 * time under heavily io.
 * 
 */

struct chunkmgr_worker_context{
    struct worker_context **wctx_array;

    uint64_t nb_max_chunks;
    uint64_t nb_used_chunks;
    uint32_t nb_pages_per_chunk;

    uint32_t nb_business_workers;
    struct spdk_thread *thread;
};

struct pagechunk_mgr{
    TAILQ_HEAD(,chunk_desc) global_chunks; 
    uint64_t nb_used_chunks;
    uint64_t hit_times;
    uint64_t miss_times;
    struct chunkmgr_worker_context *chunkmgr_worker;
    struct object_cache_pool *kv_chunk_request_pool;
    struct object_cache_pool *load_store_ctx_pool;
};

struct chunk_miss_callback{
    // The requestor shaell fill the field.
    struct pagechunk_mgr *requestor_pmgr;

    // The page chunk manager shall fill the field and
    // send the callback to the executor.
    struct pagechunk_mgr *executor_pmgr;
    struct chunk_desc* desc;

    // The executor shall fill the field by performing LRU. Or the 
    // page chunk manager worker mallocs new chunk memory directly.
    struct chunk_mem *mem;
    int kverrno;

    // The executor shall call this calback function when it finishes the
    // chunk memory allocating.
    void(*finish_cb)(void*ctx);

    // When chunk manager get a new chunk memory or error hits, the 
    // callback will be called.
    void(*cb_fn)(void*ctx,int kverrno);
    void* ctx;
    TAILQ_ENTRY(chunk_miss_callback) link;
};

struct chunk_load_store_ctx{
    struct pagechunk_mgr *pmgr;
    struct chunk_desc *desc;
    uint64_t slot_idx;
    uint32_t first_page;
    uint32_t last_page;

    //For shared page loading only
    uint32_t cnt;
    uint32_t nb_segs;
    int kverrno;

    void(*user_cb)(void*ctx, int kverrno);
    void* user_ctx;
};

void chunkmgr_request_one_aysnc(struct chunk_miss_callback *cb_obj);

// I needn't care when and where the mem is released by page chunk manager
// worker, so the function is designed as a sync function.
// The only thing I need to do is telling the manager worker that I want to
// release a chunk memory. The concret releasing will be posted to background.
void chunkmgr_release_one(struct pagechunk_mgr* pmgr,struct chunk_mem* mem);

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
    uint32_t nb_faults;

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

int worker_reclaim_process_pending_item_delete(struct worker_context *wctx);
int worker_reclaim_process_pending_item_migrate(struct worker_context *wctx);
int worker_reclaim_process_pending_slab_migrate(struct worker_context *wctx);

/*----------------------------------------------------*/
//Recovery related
//
void worker_perform_rebuild_async(struct worker_context *wctx, 
                                  void(*complete_cb)(void*ctx, int kverrno),void*ctx);

#endif
