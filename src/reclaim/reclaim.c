#include "reclaim.h"
#include "slab.h"
#include "kvutil.h"

struct reclaim_node* reclaim_alloc_one_node(struct slab* slab,uint32_t node_id){
   
    uint32_t nb_chunks = slab->reclaim.nb_chunks_per_node;
    uint32_t bitmap_data_size = KV_ALIGN(slab->reclaim.nb_slots_per_chunk/8 + 1, 4);
    uint32_t chunk_size = sizeof(struct chunk_desc) + bitmap_data_size;
    uint32_t total_size = sizeof(struct reclaim_node) + (chunk_size + sizeof(void*)) * nb_chunks;

    uint8_t *mem = calloc(total_size,1);
    if(!mem){
        return NULL;
    }

    struct reclaim_node* node = mem;
    node->id = node_id;
    node->nb_free_slots = slab->reclaim.nb_slots_per_chunk * slab->reclaim.nb_chunks_per_node;
    
    int i = 0;
    struct chunk_desc *desc;
    uint8_t *desc_data;

    node->desc_array = (struct chunk_desc**)(node+1);
    desc_data = (uint8_t*)node->desc_array + sizeof(void*) * nb_chunks;

    for(;i<slab->reclaim.nb_chunks_per_node;i++){
        desc = (struct chunk_desc*)(desc_data + chunk_size*i);
        desc->id             = node->id * slab->reclaim.nb_chunks_per_node + i;
        desc->nb_free_slots  = slab->reclaim.nb_slots_per_chunk;
        desc->nb_pages       = slab->reclaim.nb_pages_per_chunk;
        desc->nb_slots       = slab->reclaim.nb_slots_per_chunk;
        desc->slab           = slab;
        desc->slab_size      = slab->slab_size;
        desc->bitmap         = (struct bitmap*)(desc+1);
        desc->bitmap->length = slab->reclaim.nb_slots_per_chunk;
        desc->bitmap->data   = (uint8_t*)(desc->bitmap+1);

        node->desc_array[i]  = desc;
    }
}

bool reclaim_evaluate_slab(struct slab* slab){
    //todo 
    return false;
}