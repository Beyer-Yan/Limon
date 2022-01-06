#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include "pool.h"
#include "kvutil.h"

#define POOL_FINGERPTINR (123456787654321)

static_assert(sizeof(struct object_cache_pool)==48,"incorrect size");


struct object_cache_pool* pool_create(uint32_t count, uint32_t object_size){
    struct object_cache_pool* pool;
    pool = malloc(sizeof(*pool) + count*sizeof(uint64_t) + count*object_size);
    assert(pool);

    pool->key = POOL_FINGERPTINR;
    pool->count = count;
    pool->object_size = object_size;
    pool->nb_frees = count;

    pool->cache_data = (uint8_t*)(pool+1) + count*sizeof(uint64_t);
    pool->head = 0;

    for(uint32_t i=0;i<count;i++){
         pool->free_node_array[i] = i+1;
    } 
    pool->free_node_array[count-1] = UINT64_MAX;

    return pool;
}

void pool_destroy(struct object_cache_pool* pool){
    assert(pool);
    assert(pool->key==POOL_FINGERPTINR);
    free(pool);
}


void* pool_get(struct object_cache_pool *pool){
    assert(pool);
    assert(pool->key==POOL_FINGERPTINR);

    if(!pool->nb_frees){
        return NULL;
    }

    uint64_t head = pool->head;
    pool->head = pool->free_node_array[head];
    pool->free_node_array[head] = UINT64_MAX;
    pool->nb_frees--;
    
    return pool->cache_data + head*pool->object_size;
}


void pool_release(struct object_cache_pool *pool, void* object){
    assert(pool);
    assert(pool->key==POOL_FINGERPTINR);    

    uint64_t object_idx = ((uint64_t)object-(uint64_t)pool->cache_data)/pool->object_size;
    uint64_t head = pool->head;
    assert(object_idx<pool->count && pool->free_node_array[object_idx]==UINT64_MAX);

    pool->free_node_array[object_idx] = head;
    pool->head = object_idx;
    pool->nb_frees++;
}