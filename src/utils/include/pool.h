#ifndef KVS_POOL_H
#define KVS_POOL_H

#include <stdbool.h>
#include <stdint.h>
#include <assert.h>

struct object_node{
     void* object;
     uint64_t next;
};

struct object_cache_pool{
    //How many objects in this pool
    uint64_t count;   

    //Free objects
    uint64_t nb_frees;
    // size in bytes of each object           
    uint64_t object_size;         

    //object cache data      
    uint8_t *cache_data;     

    //Free objects are linked by list.
    struct object_node free_node_array[0];     
};

static_assert(sizeof(struct object_cache_pool)==32,"incorrect size");

uint64_t pool_header_size(uint64_t count);

//Return the total size of the pool incluing header and object data.
uint64_t pool_header_init(struct object_cache_pool *pool, uint64_t count,uint64_t object_size,
                      uint64_t header_size,uint8_t *object_data);
                      
void* pool_get(struct object_cache_pool *pool);
bool pool_release(struct object_cache_pool *pool, void* object);

#endif
