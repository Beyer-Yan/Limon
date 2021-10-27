#ifndef KVS_WORKER_INTERNAL_H
#define KVS_WORKER_INTERNAL_H

#include <stddef.h>
#include "index.h"
#include "worker.h"
#include "slab.h"
#include "pool.h"
#include "io.h"
#include "pagechunk.h"
#include "kvutil.h"
#include "mtable.h"

#include "spdk/thread.h"
#include "spdk/env.h"

enum op_code { GET=0, PUT, DELETE};

struct worker_context;


/*----------------------------------------------------*/
//Resolving for conflicts operation for the same item.

struct req_conflict;

struct req_conflict* conflict_new(void);
void conflict_destroy(struct req_conflict* conflicts);

/**
 * @brief Check the conflict state for the given item
 * 
 * @param conflicts  the conflicts object
 * @param item       the given item
 * @param op         the op code, GET,PUT,DELETE
 * @return uint32_t  0 for conflict, otherwise the allocated conflict bucket
 */
uint32_t conflict_check_or_enter(struct req_conflict* conflicts, struct kv_item *item,enum op_code op);

void conflict_leave(struct req_conflict* conflicts, uint32_t bucket,enum op_code op);


/*----------------------------------------------------*/
//For normal KV operations

struct process_ctx{
    struct worker_context *wctx;
    struct slab* slab;
    struct chunk_desc* desc;
    struct slot_entry* entry;

    //used for add new item.
    uint32_t conflict_bucket;
    
    //When an item is updated not in place, this field will be used to state
    int slab_changed;
    struct slab* new_slab;
    struct chunk_desc* new_desc;
    struct slot_entry new_entry;
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
    uint64_t sid;
    kv_cb cb_fn;
    void* ctx;

} __attribute__(( aligned(CACHE_LINE_LENGTH) ));

struct kv_request_internal{
    TAILQ_ENTRY(kv_request_internal) link;
    uint32_t op_code;
    uint32_t shard;

    struct kv_item* item;
    uint64_t sid;
    kv_cb cb_fn;
    void* ctx;

    struct process_ctx pctx;
};


struct worker_context{
    volatile bool ready;

    //The shards points to the shards field of kvs structure.
    struct slab_shard *shards;
    uint32_t nb_shards;
    uint32_t core_id;

    //this is a global index handle of g_kvs;
    struct mem_index* global_index;

    //The migrating of shards below are processed by this worker. 
    uint32_t nb_reclaim_shards;
    uint32_t reclaim_shards_start_id;
    uint32_t reclaim_percentage_threshold;

    uint32_t io_cycle; //us
    
    uint32_t max_pending_kv_request;
    struct spdk_ring *req_ring;  
    struct object_cache_pool *kv_request_internal_pool;
    // thread unsafe queue, but we do not care the unsafety in single thread program
    TAILQ_HEAD(, kv_request_internal) submit_queue;
    TAILQ_HEAD(, kv_request_internal) resubmit_queue;

    //Resolve add conflicts for the same item.
    struct req_conflict* add_conflicts;

    struct mtable* mtable;
    
    //poller to process internal requests, the poller will be run 
    struct spdk_poller *request_poller;
    struct spdk_poller *io_poller;
    struct spdk_poller* slab_evaluation_poller;

    struct pagechunk_mgr *pmgr;
    struct reclaim_mgr   *rmgr;
    struct iomgr         *imgr;

    struct spdk_thread* thread;
    
} __attribute__(( aligned(CACHE_LINE_LENGTH) ));

void worker_process_get(struct kv_request_internal *req);
void worker_process_put(struct kv_request_internal *req);
void worker_process_delete(struct kv_request_internal *req);
void worker_process_rmw(struct kv_request_internal *req);

/*----------------------------------------------------*/
//page chunk related
/**
 * @brief The page chunk manager thead is a special thread to manager all chunk memory。
 * There is only one page chunk manager thead in the system. It is recommended to spawn
 * the thread on a special cpu core in case that the chunk request is not process in
 * time under heavily io.
 * 
 */

struct meta_worker_context{
    struct worker_context **wctx_array;
    uint32_t nb_business_workers;

    struct spdk_thread *meta_thread;
    struct spdk_io_channel* channel;
    struct spdk_blob_store* target;
    struct spdk_poller* stat_poller;
    
    uint32_t core_id;
};

struct pagechunk_mgr{
    TAILQ_HEAD(,page_desc) pages_head;
    uint64_t nb_init_pages;
    uint64_t nb_used_pages;
    //suggested mem chunks that I can use. Normally, I shoud not 
    //request mem chunks larger than the water_mark. But If the
    //work is busy enough, I should try to apply more mem chunks from 
    //chunk manager.
    uint64_t water_mark;
    uint64_t visit_times;
    uint64_t miss_times;
    uint32_t seed; //for remote eviction

    map_t page_map;
    uint8_t* cache_base_addr;
    struct page_desc* pdesc_arr;

    struct spdk_thread *meta_thread;

    struct dma_buffer_pool* dma_pool;
    struct object_cache_pool *remote_page_request_pool;
    struct object_cache_pool *load_store_ctx_pool;
    struct object_cache_pool* item_ctx_pool;
};

struct remote_page_request{
    // The requestor shaell fill the field.
    struct pagechunk_mgr *requestor_pmgr;

    // The page chunk manager shall fill the field and
    // send the callback to the executor.
    struct pagechunk_mgr *executor_pmgr;

    // The executor shall fill the field by performing LRU. Or the 
    // page chunk manager worker mallocs new chunk memory directly.
    uint32_t nb_pages;
    struct page_desc** pdesc_arr;
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

void meta_request_remote_pages_aysnc(struct remote_page_request *req);

/*----------------------------------------------------*/
//Reclaim related

typedef void (*reclaim_io_cb)(void* ctx, int kverrno);

struct pending_item_migrate{
    struct worker_context* wctx;
    struct slab *slab;
    uint64_t sid;

    struct chunk_desc *desc;
    struct slot_entry* entry;
    uint32_t shard_idx;
    uint32_t slab_idx;
    uint64_t slot_idx;

    //slot migration is only allowed intra slab
    struct chunk_desc* new_desc; 
    uint32_t new_slot_idx;

    reclaim_io_cb io_cb_fn;
    void *ctx;

    TAILQ_ENTRY(pending_item_migrate) link;
};

//Each pending_slab_migrate operations includes multi pending_item_migrate operations.
struct slab_migrate_request{
    uint32_t shard_id;
    uint32_t slab_idx;
    struct slab* slab;
    struct reclaim_node* node;
    struct worker_context *wctx;

    //If a fault hits, I should abort the migrating.
    bool is_fault;
    uint32_t nb_faults;
    uint64_t nb_processed;
    uint64_t nb_valid_slots;
    uint64_t start_slot;
    uint64_t cur_slot;
    uint64_t last_slot;

    TAILQ_ENTRY(slab_migrate_request) link;
};

struct reclaim_mgr{
    uint32_t migrating_batch;
    uint32_t reclaim_percentage_threshold;
    uint32_t nb_pending_slots;

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

    struct object_cache_pool *pending_migrate_pool;
    struct object_cache_pool *migrate_slab_pool;
};

int worker_reclaim_process_pending_item_migrate(struct worker_context *wctx);
int worker_reclaim_process_pending_slab_migrate(struct worker_context *wctx);

/*----------------------------------------------------*/
//Recovery related
//
void worker_perform_rebuild_async(struct worker_context *wctx, 
                                  void(*complete_cb)(void*ctx, int kverrno),void*ctx);

#endif
