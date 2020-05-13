#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include "pool.h"
#include "kvutil.h"

uint64_t pool_header_size(uint32_t count){
    uint64_t size;
    size  = sizeof(struct object_cache_pool) + (count+1)*sizeof(struct object_node);
    return size;
}

void pool_header_init(struct object_cache_pool *pool, uint32_t count,uint32_t object_size,
                      uint64_t header_size,uint8_t *object_data){
    assert(pool!=NULL);
    assert(object_data!=NULL);
    assert((uint64_t)object_data%4!=0);
    assert(header_size>=pool_header_size(count));

    pool->count = count;
    pool->object_size = object_size;
    pool->free_node_array = (struct object_node*)(pool+1);

    pool->free_node_array[count].object = NULL;
    pool->free_node_array[count].next = 0;

    pool->cache_data = object_data;

    int i = 0;
    for(;i<count;i++){
         pool->free_node_array[i].next = i+1;
         pool->free_node_array[i].object = (void*)(pool->cache_data[i*pool->object_size]);
    }
    pool->nb_frees = count;
}

void* pool_get(struct object_cache_pool *pool){
    uint32_t head = pool->count;
    void* object;
    if(!pool->nb_frees){
        object =  NULL;
    }else{
        int free_idx = pool->free_node_array[head].next;
        pool->free_node_array[head].next = pool->free_node_array[free_idx].next;
        pool->free_node_array[free_idx].next = -1;
        pool->nb_frees--;
        object = pool->free_node_array[free_idx].object;;
    }
    return object;
}

bool pool_release(struct object_cache_pool *pool, void* object){
    uint32_t object_idx = ((uint64_t)object-(uint64_t)(pool->cache_data))/pool->object_size;
    uint32_t head = pool->count;

    if(object_idx>=pool->count || pool->free_node_array[object_idx].next!=-1){
        // object is not valid.
        return false;
    }

    pool->free_node_array[object_idx].next = pool->free_node_array[head].next;
    pool->free_node_array[head].next = (int)object_idx;
    pool->nb_frees++;

    return true;
}