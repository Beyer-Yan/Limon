#ifndef KVS_RECLAIM_H
#define KVS_RECLAIM_H

#include <stdint.h>
#include <assert.h>
#include <stdalign.h>
#include "queue.h"
#include "uthash.h"
#include "rbtree_uint.h"
#include "iomgr.h"

struct slab;

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

static_assert(sizeof(struct reclaim_node)==8,"incorrect size");

struct reclaim_node* reclaim_alloc_one_node(struct slab* slab,uint32_t node_id);

//Reclaim thread for background item reclaiming and migrating.
struct reclaim_mgr;

extern void worker_reclaim_post_deleting(struct reclaim_mgr* rmgr,
                          struct slab* slab, 
                          uint64_t slot_idx,
                          void (*cb)(void* ctx, int kverrno),
                          void* ctx);

inline uint32_t reclaim_get_start_chunk_id(struct slab_reclaim* r, struct reclaim_node *n){
    return r->nb_chunks_per_node * n->id;
}

inline uint64_t reclaim_get_start_slot(struct slab_reclaim* r, struct reclaim_node* node){
    uint64_t slot_idx;
    slot_idx = r->nb_chunks_per_node*r->nb_slots_per_chunk*node->id;
    return slot_idx;
}

inline void reclaim_free_node(struct slab_reclaim* r, struct reclaim_node* node){
    assert(node!=NULL);
    rbtree_delete(r->total_tree,node->id,NULL);
    free(node);
}

bool reclaim_evaluate_slab(struct slab* slab);

#endif

