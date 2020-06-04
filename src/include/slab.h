#ifndef KVS_SLAB_H
#define KVS_SLAB_H

#include <stdint.h>
#include <stdbool.h>
#include <assert.h>
#include "rbtree_uint.h"
#include "io.h"

#include "spdk/blob.h"

#define SLAB_FLAG_RECLAIMING 1

/**
 * @brief When the size of a item is lager than MULTI_PAGE_SLAB_SIZE,
 * it is allowed to storage in multi pages. Items less than 
 * MULTI_PAGE_SLAB_SIZE will alway be put into only one page.
 */
#define MULTI_PAGE_SLAB_SIZE   768u
#define CHUNK_SIZE             135u
#define KVS_PAGE_SIZE          4096u
#define MAX_SLAB_SIZE          4*1024*1024u

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

// runtime data structure for slab

struct chunk_desc;
struct reclaim_mgr;

struct reclaim_node{
    uint32_t id;
    uint32_t nb_free_slots;
    struct chunk_desc *desc_array[0];
};

struct slab_reclaim{
    uint32_t nb_reclaim_nodes;
    uint32_t nb_chunks_per_node;
    uint32_t nb_slots_per_chunk;
    uint32_t nb_pages_per_chunk;

    uint64_t nb_total_slots;
    uint64_t nb_free_slots;

    /**
     * @brief All the reclaim node are orgnized by ordered treemap. When a slab size is increased,
     * the new reclaim node will be inserted into the map. When  there are
     * slots available  in a reclaim node, the node will be inserted in free hash. When a reclaim
     * node is fully utilized, it will be removed from the free_hash. When a slab is needed to
     * compact, the last reclaim node will be migrated into other available reclaim node.
     * The compacted reclaim node will be removed from total_tree and free_node_tree. Lastly, the 
     * disk space of items from the reclaim node will be freed.
     * 
     */
    rbtree total_tree;
    rbtree free_node_tree;
};

//Used when the slab is resized.
struct resize_ctx;

struct slab {
    uint32_t slab_size;
    struct spdk_blob *blob;
    uint32_t flag;
    struct slab_reclaim reclaim;
    TAILQ_HEAD(,resize_ctx) resize_head;
};

struct slab_shard{
    uint32_t nb_slabs;
    struct slab* slab_set;
};

static_assert(sizeof(struct slab_layout)==24,"size incorrect");
static_assert(sizeof(struct super_layout)==32,"size incorrect");
static_assert(sizeof(struct reclaim_node)==8,"incorrect size");

/**
 * @brief Get the slot offset of the slot_idx from 0.
 * 
 * @param slab       The slab.
 * @param slot_idx   The slot index.
 * @return uint32_t  The offset of the slot_idx.
 */
static inline uint32_t slab_slot_offset(struct slab*slab, uint64_t slot_idx){
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

static inline struct reclaim_node* slab_get_node(struct slab*slab, uint64_t slot_idx){
    uint32_t nb_slots_per_node = slab->reclaim.nb_chunks_per_node*slab->reclaim.nb_slots_per_chunk;
    uint32_t node_idx = slot_idx/nb_slots_per_node;
    return rbtree_lookup(slab->reclaim.total_tree,node_idx);
}

/**
 * @brief  Calculate how many slots for the given number of pages and slab size.
 * 
 * @param chunk_pages    Number of chunk pages
 * @param slab_size      Slot size
 * @return uint32_t      Return number of slots.
 */
static inline uint32_t slab_get_chunk_slots(uint32_t chunk_pages, uint32_t slab_size){
    uint32_t nb_slots;
    if(slab_size<MULTI_PAGE_SLAB_SIZE){
        //Slab with such size will be store in one page.
        nb_slots = (KVS_PAGE_SIZE/slab_size)*chunk_pages;
    }
    else{
        nb_slots = chunk_pages*KVS_PAGE_SIZE/slab_size;
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
 * @brief Judge whether the given item size is legal for the given slab size.
 * 
 * @param slab_size  The slab size.
 * @param item_size  The item size.
 * @return true      Valid
 * @return false     Invalid.
 */
bool slab_is_valid_size(uint32_t slab_size, uint32_t item_size);

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

void slab_create_async(struct iomgr* imgr,
                       uint32_t slab_size, 
                       char* slab_name, 
                       void (*cb)(struct slab* slab, void* ctx,int kverrno),
                       void* ctx);
*/

/**
 * @brief Truncate the slab. The unit is mulitple of size of reclaim node. Data
 * will not be migrated automatically when the slab is truncated, which means that the user
 * shall migrate the data before truncating the slab.
 * The minimum truncating size if one reclaim node.
 * Attention that the user shall process the page chunk and slab reclaim data. The slab truncating
 * only performs disk data truncating.
 * 
 * @param imgr      The io manager. The actual disk IO is managed by the iomgr.
 * @param slab      The slab to be resized
 * @param nb_nodes  The number of reclaim nodes to be truncated.
 * @param cb        User callback
 * @param ctx       Parameter of user callback
 */
void slab_truncate_async(struct iomgr* imgr,
                       struct slab* slab,
                       uint64_t nb_nodes,
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

/**
 * @brief Get the start chunk id for the given reclaim node.
 * 
 * @param r          The slab reclaim.
 * @param n          The reclaim node.
 * @return uint32_t  The start chunk id.
 */
static inline uint32_t slab_reclaim_get_start_chunk_id(struct slab_reclaim* r, struct reclaim_node *n){
    return r->nb_chunks_per_node * n->id;
}

/**
 * @brief  Get the start slot index for the given reclaim node.
 * 
 * @param r          The slab reclaim.
 * @param node       The reclaim node.
 * @return uint64_t  The start slot index.
 */
static inline uint64_t slab_reclaim_get_start_slot(struct slab_reclaim* r, struct reclaim_node* node){
    uint64_t slot_idx;
    slot_idx = r->nb_chunks_per_node*r->nb_slots_per_chunk*node->id;
    return slot_idx;
}

/**
 * @brief Free a reclaim node.
 * 
 * @param r     The slab_reclaim.
 * @param node  The slab reclaim node to be freed.
 */
static inline void slab_reclaim_free_node(struct slab_reclaim* r, struct reclaim_node* node){
    assert(node!=NULL);
    rbtree_delete(r->total_tree,node->id,NULL);
    free(node);
}

/**
 * @brief Allocate one reclaim node. The function is used when the slab needs resizing.
 * 
 * @param slab                  The slab runtime.
 * @param node_id               The reclaim node id. It will be assigned to the new node.
 * @return struct reclaim_node* Poitner of the newly allocated reclaim node.
 */
struct reclaim_node* slab_reclaim_alloc_one_node(struct slab* slab,uint32_t node_id);

/**
 * @brief Evaluate whether the given slab needs performing reclaim.
 * 
 * @param         The slab      
 * @return true   The slab needs performing reclaim.
 * @return false  The slab does not need performing reclaim.
 */
bool slab_reclaim_evaluate_slab(struct slab* slab);

extern void slab_reclaim_post_delete(struct reclaim_mgr* rmgr,
                          struct slab* slab, 
                          uint64_t slot_idx,
                          void (*cb)(void* ctx, int kverrno),
                          void* ctx);

#endif
