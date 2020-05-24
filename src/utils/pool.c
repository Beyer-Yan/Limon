#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include "pool.h"
#include "kvutil.h"

uint64_t pool_header_size(uint64_t count){
    //The pool size is designed less than UINT64_MAX;
    assert(count<UINT64_MAX);

    uint64_t size;
    size  = sizeof(struct object_cache_pool) + (count+1)*sizeof(struct object_node);
    return size;
}

uint64_t pool_header_init(struct object_cache_pool *pool, uint64_t count,uint64_t object_size,
                      uint64_t header_size,uint8_t *object_data){
    assert(pool!=NULL);
    assert(object_data!=NULL);
    assert((uint64_t)object_data%8==0);
    assert(header_size>=pool_header_size(count));

    pool->count = count;
    pool->object_size = object_size;

    pool->free_node_array[count].object = NULL;
    pool->free_node_array[count].next = 0;

    pool->cache_data = object_data;

    uint32_t i = 0;
    for(;i<count-1;i++){
         pool->free_node_array[i].next = i+1;
         pool->free_node_array[i].object = (void*)(&pool->cache_data[i*pool->object_size]);
    }
    //The header always point the first free node.
    pool->free_node_array[count-1].next = UINT64_MAX;
    pool->free_node_array[count].next   = 0;
    pool->nb_frees = count;

    return header_size + count*object_size;
}

void* pool_get(struct object_cache_pool *pool){
    uint64_t head = pool->count;
    void* object;
    if(!pool->nb_frees){
        object =  NULL;
    }else{
        uint64_t free_idx = pool->free_node_array[head].next;
        pool->free_node_array[head].next = pool->free_node_array[free_idx].next;
        pool->free_node_array[free_idx].next = UINT64_MAX;
        pool->nb_frees--;
        object = pool->free_node_array[free_idx].object;
    }
    return object;
}

bool pool_release(struct object_cache_pool *pool, void* object){
    uint64_t object_idx = ((uint64_t)object-(uint64_t)pool->cache_data)/pool->object_size;
    uint64_t head = pool->count;

    // object is not valid.
    assert(object_idx<pool->count && pool->free_node_array[object_idx].next==UINT64_MAX);
     
    pool->free_node_array[object_idx].next = pool->free_node_array[head].next;
    pool->free_node_array[head].next = object_idx;
    pool->nb_frees++;

    return true;
}
