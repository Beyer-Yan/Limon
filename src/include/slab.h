#ifndef KVS_SLAB_H
#define KVS_SLAB_H

#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include "reclaim.h"
#include "pagechunk.h"
#include "rbtree_uint.h"
#include "iomgr.h"

#include "spdk/blob.h"

#define SLAB_FLAG_RECLAIMING 1

/**
 * @brief When the size of a item is lager than MULTI_PAGE_SLAB_SIZE,
 * it is allowed to storage in multi pages. Items less than 
 * MULTI_PAGE_SLAB_SIZE will alway be put into only one page.
 */
#define MULTI_PAGE_SLAB_SIZE 768

#define KVS_PAGE_SIZE 4096
#define MAX_SLAB_SIZE 4*1024*1024

//In disk layout

struct slab_layout{
    uint32_t slab_size;
    uint64_t blob_id;
    uint64_t resv;
}; 

#define DEFAULT_KVS_PIN 0x1022199405121993u

struct super_layout{
    //If the kvs_pin is not equal to DEFAULT_KVS_PIN, it will be a invalid.
    uint64_t kvs_pin;
    uint32_t nb_shards;
    uint32_t nb_slabs_per_shard;
    uint32_t nb_chunks_per_reclaim_node;
    uint32_t nb_pages_per_chunk;
    uint32_t max_key_length;
    struct slab_layout slab[0];
};

static_assert(sizeof(struct slab_layout)==24,"size incorrect");
static_assert(sizeof(struct super_layout)==32,"size incorrect");

// runtime data structure for slab
struct slab {
    uint32_t slab_size;
    struct spdk_blob *blob;

    uint32_t flag;

    struct slab_reclaim reclaim;
};

struct slab_shard{
    uint32_t nb_slabs;
    struct slab* slab_set;
};

/**
 * @brief Get the hints of the given slot.
 * 
 * @param slab       The slab runtime object.
 * @param slot_idx   The slot index.
 * @param node_out   The returned reclaim node.
 * @param desc_out   The returned page chunk pointer.
 * @param slot_offset The returned slot offset in the page chunk.
 */
inline void slab_get_hints(struct slab*slab, uint64_t slot_idx, 
                struct reclaim_node** node_out,
                struct chunk_desc **desc_out,
                uint64_t *slot_offset ){
    
    struct slab_reclaim *r = &slab->reclaim;

    uint32_t node_id      = slot_idx/r->nb_slots_per_chunk/r->nb_chunks_per_node;
    uint32_t chunk_offset = slot_idx/r->nb_slots_per_chunk%r->nb_chunks_per_node;
    uint64_t offset       = slot_idx%r->nb_slots_per_chunk;

    *node_out    = rbtree_lookup(r->total_tree,node_id);
    *desc_out    = (*node_out)->desc_array[chunk_offset];
    *slot_offset = offset;
}

/**
 * @brief Get the slot offset of the slot_idx from 0.
 * 
 * @param slab       The slab.
 * @param slot_idx   The slot index.
 * @return uint32_t  The offset of the slot_idx.
 */
inline uint32_t slab_slot_offset(struct slab*slab, uint64_t slot_idx){
    uint32_t slab_size = slab->slab_size;
    if(slab_size<MULTI_PAGE_SLAB_SIZE){
        uint32_t slots_per_page = KVS_PAGE_SIZE/slab_size;
        uint32_t first_page = slot_idx/slots_per_page;
        uint32_t offset_in_page = slot_idx%slots_per_page;
        return first_page*KVS_PAGE_SIZE + offset_in_page*slab_size;
    }
    else{
        return slab_size*slot_idx;
    }
}

/**
 * @brief  Calculate how many slots for the given number of pages and slab size.
 * 
 * @param chunk_pages    Number of chunk pages
 * @param slab_size      Slot size
 * @return uint32_t      Return number of slots.
 */
inline uint32_t slab_get_chunk_slots(uint32_t chunk_pages, uint32_t slab_size){
    uint32_t nb_slots;
    if(slab_size<MULTI_PAGE_SLAB_SIZE){
        //Slab with such size will be store in one page.
        nb_slots = (KVS_PAGE_SIZE/slab_size)*chunk_pages;
    }
    else{
        nb_slots = chunk_pages/slab_size;
    }
    return nb_slots;
}

/**
 * @brief Find the best slab for given item size.
 * 
 * @param item_size   The item size
 * @return uint32_t        Return the slab index. Crash if no situable slab is found.
 */
uint32_t slab_find_slab(uint32_t item_size);

/**
 * @brief Get the slab configuration fixed in the code.
 * 
 * @param slab_size_array     The returned slab array
 * @param nb_slabs            The returned number of slabs
 * @param chunk_pages         The returned chunk size.
 */
void slab_get_slab_conf(uint32_t **slab_size_array, uint32_t *nb_slabs, uint32_t *chunk_pages);

/**
 * @brief Check whether the slab is changed for an item of which size is changed.
 * 
 * @param old_item_size    The old size of the changed item
 * @param new_item_size    The new size of the changed item
 * @return true            The slab index is changed
 * @return false           The slab index is not changed
 */
bool slab_is_slab_changed(uint32_t old_item_size, uint32_t new_item_size);

/**
 * @brief  Judge whether the given slot has been allocated.
 * 
 * @param slab      The slab
 * @param slot_idx  The given slot index
 * @return true     The slot is occupied.
 * @return false    The slot is not occupied.
 */
bool slab_is_slot_occupied(struct slab* slab,uint64_t slot_idx);

/**
 * @brief Create a new slab including both persistent data and in memory data structure.
 * 
 * @param imgr       The io manager. The actual disk IO is managed by the iomgr.
 * @param slab_size  The initial slab size. The size must be the multiple of reclaim node size.
 * @param slab_name  The name for the slab. It will be persisted into disk.
 * @param cb         User callback.
 * @param ctx        Parameter of user callback.
 */
void slab_create_async(struct iomgr* imgr,
                       uint32_t slab_size, 
                       char* slab_name, 
                       void (*cb)(struct slab* slab, void* ctx,int kverrno),
                       void* ctx);

/**
 * @brief Extent or truncate the slab. The unit is mulitple of size of reclaim node. Data
 * will not be migrated automatically when the slab is truncated, which means that the user
 * shall migrate the data before truncating the slab.
 * 
 * @param imgr      The io manager. The actual disk IO is managed by the iomgr.
 * @param slab      The slab to be resized
 * @param new_size  The newly setting size
 * @param cb        User callback
 * @param ctx       Parameter of user callback
 */
void slab_resize_async(struct iomgr* imgr,
                       struct slab* slab,
                       uint64_t new_size,
                       void (*cb)(void* ctx, int kverrno),
                       void* ctx);

/**
 * @brief Request an unoccupied slot from the corresponding slab. This may cause slab reszing
 * when the slab has no enough space.
 * 
 * @param imgr      The io manager. The actual disk IO is managed by the iomgr.
 * @param slab      The slab from which the request works.
 * @param cb        User callback.
 * @param ctx       Parameter of user callback.  
 */
void slab_request_slot_async(struct iomgr* imgr,
                             struct slab* slab, 
                             void (*cb)(uint64_t slot_idx, void* ctx, int kverrno), 
                             void* ctx);

/**
 * @brief Delete the slot asynchronized. Deleting a slot means writing tombstone of the 
 * corresponding item into the page and clear the bitmap. The deleting will be post to 
 * background reclaiming thread.
 * 
 * @param rmgr      The reclaim manager that the deleting will posted to.
 * @param slab      The slab object pointer
 * @param slot_idx  The slot index to be deleted
 * @param cb        User callback when delete successes. NULL means that user does not care the reclaiming,
 * which means that the reclaiming is posted into background stage. 
 * @param ctx       Parameter of user callback. When cb is NUll, the ctx will not be discarded.
 */
void slab_free_slot_async(struct reclaim_mgr* rmgr,
                          struct slab* slab, 
                          uint64_t slot_idx,
                          void (*cb)(void* ctx, int kverrno),
                          void* ctx);

#endif
