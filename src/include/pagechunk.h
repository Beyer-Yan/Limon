#ifndef KVS_PAGECHUNK_H
#define KVS_PAGECHUNK_H

#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include "bitmap.h"
#include "io.h"
#include "pool.h"
#include "slab.h"
#include "kvutil.h"

#include "spdk/queue.h"

#define CHUNK_PIN  1u

struct item_load_store_ctx{
    //For shared page loading only
    uint32_t cnt;
    uint32_t nb_segs;

    //used for storing pages
    uint64_t slot_addr_offset; 
    uint32_t size;

    int kverrno;
    void(*user_cb)(void*ctx, int kverrno);
    void* user_ctx;
};

struct page_load_store_ctx{
    struct pagechunk_mgr *pmgr;
    struct chunk_desc *desc;
    struct item_load_store_ctx* item_ctx;
    uint32_t page_offset;

    //used for storing pages
    uint32_t nb_pages;
};

struct page_desc{
    TAILQ_ENTRY(page_desc) link;
    uint64_t key;
    uint8_t* data;
};

struct chunk_desc {
    uint32_t id;
    uint32_t nb_free_slots;
    uint32_t nb_pendings;
    struct dma_buffer* dma_buf;
    struct slab* slab;
    //bitmap to record the slot allocation
    struct bitmap bitmap[0];
};

struct pagechunk_mgr;
static_assert(sizeof(struct chunk_desc)==32,"incorrect size");

/**
 * @brief Get the hints of the given slot.
 * 
 * @param slab       The slab runtime object.
 * @param slot_idx   The slot index.
 * @param node_out   The returned reclaim node.
 * @param desc_out   The returned page chunk pointer.
 * @param slot_offset The returned slot offset in the page chunk.
 */
static inline void pagechunk_get_hints(struct slab*slab, uint64_t slot_idx, 
                struct reclaim_node** node_out,
                struct chunk_desc **desc_out,
                uint64_t *slot_offset ){
    
    struct slab_reclaim *r = &slab->reclaim;

    uint32_t node_id      = slot_idx/r->nb_slots_per_chunk/r->nb_chunks_per_node;
    uint32_t chunk_offset = slot_idx/r->nb_slots_per_chunk%r->nb_chunks_per_node;
    uint64_t offset       = slot_idx%r->nb_slots_per_chunk;

    assert(node_id<r->nb_reclaim_nodes);

    *node_out    = r->node_array[node_id];
    *desc_out    = (*node_out)->desc_array[chunk_offset];
    *slot_offset = offset;
}

static inline struct chunk_desc* pagechunk_get_desc(struct slab* slab, uint64_t slot_idx){
    struct reclaim_node* node;
    struct chunk_desc* desc;
    uint64_t slot_offset;

    pagechunk_get_hints(slab,slot_idx,&node,&desc,&slot_offset);
    assert(node!=NULL);

    return desc;
}

/**
 * @brief Occupy the desc.
 * 
 * @param desc   the pagechunk description
 */
void pagechunk_mem_lift(struct pagechunk_mgr *pmgr,struct chunk_desc* desc);

/**
 * @brief Release the occupation
 * 
 * @param desc   the pagechunk description
 */
void pagechunk_mem_lower(struct pagechunk_mgr *pmgr,struct chunk_desc* desc);

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
 * @param cb       user callback
 * @param ctx      parameter of user callback
 */
void pagechunk_store_item_meta_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx);

/**
 * @brief Evict one page belong to the pmgr.
 * 
 * @param pmgr                 The page chunk manager.
 * @return struct page_desc*   The evicted page descriptor.
 */
struct page_desc* pagechunk_evict_one_page(struct pagechunk_mgr *pmgr);

#endif
