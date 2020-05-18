#ifndef KVS_PAGECHUNK_H
#define KVS_PAGECHUNK_H

#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include "queue.h"
#include "bitmap.h"
#include "iomgr.h"
#include "pool.h"
#include "slab.h"
#include "worker.h"
#include "kvutil.h"

#define CHUNK_SIZE 252u
#define CHUNK_PIN  1u


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

struct chunk_mem {
    uint32_t nb_bytes;
    uint8_t* data;
    struct bitmap bitmap[0];
};

struct chunk_desc {

    TAILQ_ENTRY(chunk_desc) link; 
    TAILQ_HEAD(, chunk_miss_callback) chunk_miss_callback_head;

    struct slab*slab;
    uint32_t id;
    uint32_t nb_pages;
    //Variables below are for fast memory access. Otherwise, I have to get 
    //the infomation by derefering the slab pointer.
    uint32_t slab_size;
    uint32_t nb_slots;
    uint32_t nb_free_slots;

    struct chunk_mem *chunk_mem;
    uint32_t freq;
    uint32_t flag;
    
    struct bitmap bitmap[0];
};

/**
 * @brief The page chunk manager thead is a special thread to manager all chunk memory。
 * There is only one page chunk manager thead in the system. It is recommended to spawn
 * the thread on a special cpu core in case that the chunk request is not process in
 * time under heavily io.
 * 
 */
TAILQ_HEAD(chunk_list_head,chunk_desc);

struct pagechunk_mgr{
    struct chunk_list_head global_chunks; 
    uint64_t nb_used_chunks;
    uint64_t hit_times;
    uint64_t miss_times;
    struct chunkmgr_worker_context *chunkmgr_worker;
    struct object_cache_pool *kv_chunk_request_pool;
    struct object_cache_pool *load_store_ctx_pool;
};

static_assert(sizeof(struct chunk_mem)==16, "incorrect size");
static_assert(sizeof(struct chunk_desc)==80,"incorrect size");

struct chunk_desc* pagechunk_get_desc(struct slab* slab, uint64_t slot_idx);

/**
 * @brief Judge whether the given slot is cached in the page chunk cache
 * 
 * @param  desc      The page chunk description
 * @param  slot_idx  The slot index in the slab
 * @return bool      true: the slot is already cached, false:the slot is not cached
 */
bool pagechunk_is_cached(struct chunk_desc *desc, uint64_t slot_idx);

/**
 * @brief Check whether the item is stored into multi pages.
 * 
 * @param desc       The page chunk description
 * @param slot_idx   The slot index in the slab
 * @return true      The item is store in multi pages
 * @return false     The item is store in only one page
 */
bool pagechunk_is_cross_page(struct chunk_desc *desc, uint64_t slot_idx);

/**
 * @brief Get the item from page chunk cache. Be sure the cache existes by 
 * call pagechunk_is_cached.
 * 
 * @param pmgr      The page chunk manager
 * @param desc      the page chunk description
 * @param slot_idx  the slot index in the slab
 * @return kv_item* NULL:slot is not cached, otherwise rerurn the cached item data.
 */
struct kv_item* pagechunk_get_item(struct pagechunk_mgr *pmgr,struct chunk_desc *desc, uint64_t slot_idx);

/**
 * @brief Put the item into the page chunk cache. This function does not persist 
 * the item.
 * 
 * @param pmgr      The page chunk manager
 * @param desc      The page chunk description
 * @param slot_idx  The slot index in the slab
 * @param item      The item to be written into cache
 */
void pagechunk_put_item(struct pagechunk_mgr *pmgr,struct chunk_desc *desc, uint64_t slot_idx,struct kv_item* item);

struct chunk_mem* pagechunk_evict_one_chunk(struct pagechunk_mgr *pmgr);

/**
 * @brief Load data from slab at slot_idx into the corresponding position of 
 * pagechunk by iomgr.
 * 
 * @param pmgr     The page chunk manager
 * @param imgr     The io manager
 * @param desc     page chunk description
 * @param slot_idx the slot index in the slab
 * @param cb       user callback
 * @param ctx      parameter of usercallback
 */
void pagechunk_load_item_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx);

/**
 * @brief Load only the shared pages for item from slab at slot_idx into the corresponding
 * position of pagechunk by iomgr. When an item is stored, it has to be loaded to page chunk
 * cache. But for items across pages, it is unnecessary to load all the data of them. Only the
 * shared pages are needed to be loaded.
 * 
 * @param pmgr     The page chunk manager
 * @param imgr     The io manager
 * @param desc     page chunk description
 * @param slot_idx the slot index in the slab
 * @param cb       user callback
 * @param ctx      parameter of usercallback
 */
 void pagechunk_load_item_share_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx);

/**
 * @brief When an item is to be deleted, a tombstone has to be written into the meta page.
 * In such case, it is unnecessary to load all the pages of the iitem. Only the page that 
 * the meta locates shall be loaded.
 * 
 * @param pmgr     The page chunk manager
 * @param imgr     The io manager
 * @param desc     page chunk description
 * @param slot_idx the slot index in the slab
 * @param cb       user callback
 * @param ctx      parameter of usercallback
 */
void pagechunk_load_item_meta_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx);

/**
 * @brief Store data from page chunk desc for data slot_idx into the corresponding
 * positon of slab by iomgr.
 * 
 * @param pmgr     The page chunk manager
 * @param imgr     The iomanager
 * @param desc     page chunk description
 * @param slot_idx the slot index in the slab
 * @param cb       tuser callback
 * @param ctx      parameter of user callback
 */
void pagechunk_store_item_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx);

/**
 * @brief Store only the meta page of the item into disk.
 * 
 * @param pmgr     The page chunk manager
 * @param imgr     The iomanager
 * @param desc     page chunk description
 * @param slot_idx the slot index in the slab
 * @param cb       tuser callback
 * @param ctx      parameter of user callback
 */
void pagechunk_store_item_meta_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx);

/**
 * @brief Initialize the page chunk runtime object.
 * 
 * @param init_size  initial number of pagechunks.
 * @return true      init failed
 * @return false     init sucessful
 */
//bool pagechunk_init(int init_chunks);

/**
 * @brief Request a new page chunk memory. The new chunk memory will be attached in
 * the chunk description
 * 
 * @param pmgr   The page chunk manager.
 * @param desc   the page chunk description
 * @param cb     user callback function when request is processed
 * @param ctx    parameters of user callback function
 */
void pagechunk_request_one_async(struct pagechunk_mgr *pmgr,
                                 struct chunk_desc* desc,
                                 void(*cb)(void*ctx,int kverrno), 
                                 void* ctx);

/**
 * @brief Release the chunk memory to the page chunk manager. When a slab successes in
 * shrinking its size, it will call the function to release its chunk memory.
 * 
 * @param mem   The chunk memory pointer.
 */
void pagechunk_release_one(struct pagechunk_mgr *pmgr,
                            struct chunk_mem* mem);


#endif
