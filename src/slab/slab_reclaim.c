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

    struct reclaim_node* node = calloc(total_size,1);
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
        desc->nb_pages       = slab->reclaim.nb_pages_per_chunk;
        desc->nb_slots       = slab->reclaim.nb_slots_per_chunk;
        desc->slab           = slab;
        desc->slab_size      = slab->slab_size;
        desc->bitmap[0].length = slab->reclaim.nb_slots_per_chunk;
        TAILQ_INIT(&desc->chunk_miss_callback_head);
        
        node->desc_array[i]  = desc;
    }
    return node;
}

bool slab_reclaim_evaluate_slab(struct slab* slab){
    //todo 
    assert(slab!=NULL);
    return false;
}
