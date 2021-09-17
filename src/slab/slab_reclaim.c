#include "slab.h"
#include "pagechunk.h"
#include "kvutil.h"

struct reclaim_node* slab_reclaim_alloc_one_node(struct slab* slab,uint32_t node_id){
   
    uint32_t nb_chunks = slab->reclaim.nb_chunks_per_node;
    uint32_t bitmap_size = bitmap_header_size(slab->reclaim.nb_slots_per_chunk);
    uint32_t chunk_size = sizeof(struct chunk_desc) + bitmap_size;
    uint32_t total_size = sizeof(struct reclaim_node) + (chunk_size + sizeof(void*)) * nb_chunks;

    /**
     * @brief Ensure that the reclaim_node is 8 bytes aligned, the bitmap is 8 bytes aligned
     * and the chunk_desc is 8 bytes aligned.
     * Since the reclaim node is accessed very often, align mismatch of pointer will be rather costly.
     */

    struct reclaim_node* node = calloc(1,total_size);
    if(!node){
        return NULL;
    }

    node->id = node_id;
    node->nb_free_slots = slab->reclaim.nb_slots_per_chunk * slab->reclaim.nb_chunks_per_node;
    
    uint32_t i = 0;
    struct chunk_desc *desc;
    struct chunk_desc **desc_base = node->desc_array +  nb_chunks;

    for(;i<slab->reclaim.nb_chunks_per_node;i++){
        desc                 = (struct chunk_desc*)(desc_base + chunk_size*i/sizeof(struct chunk_desc*));
        desc->id             = node->id * slab->reclaim.nb_chunks_per_node + i;
        desc->nb_free_slots  = slab->reclaim.nb_slots_per_chunk;
        desc->slab           = slab;
        desc->nb_pendings    = 0;
        desc->bitmap[0].length = slab->reclaim.nb_slots_per_chunk;
        node->desc_array[i]  = desc;
    }
    return node;
}


bool slab_reclaim_evaluate_slab(struct slab* slab){
    //todo 
    assert(slab!=NULL);
    //@TODO slab fragmentation evaluation

    //for simulation
    if(slab->reclaim.nb_reclaim_nodes>10){
        return true;
    }
    return false;
}

void slab_free_slot(struct reclaim_mgr* rmgr,
                          struct slab* slab, 
                          uint64_t slot_idx){

    struct chunk_desc* desc = pagechunk_get_desc(slab,slot_idx);
    assert(desc!=NULL);

    uint32_t slot_offset      = slot_idx%slab->reclaim.nb_slots_per_chunk;
    uint32_t node_id          = slot_idx/slab->reclaim.nb_slots_per_chunk/slab->reclaim.nb_chunks_per_node;

    assert(node_id<slab->reclaim.nb_reclaim_nodes);
    struct reclaim_node *node = slab->reclaim.node_array[node_id];
    bitmap_clear_bit(desc->bitmap,slot_offset);

    if(!node->nb_free_slots){
        //This node is full node. But now, there is a empty slot for it. So I should put it
        //in free_node treemap;
        rbtree_insert(slab->reclaim.free_node_tree,node_id,node,NULL);
    }

    desc->nb_free_slots++;
    node->nb_free_slots++;
    slab->reclaim.nb_free_slots++;
}

