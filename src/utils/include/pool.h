#ifndef __POOL_H
#define __POOL_H

#include <stdbool.h>

struct object_node{
     void* object;
     int next;
};

struct object_cache_pool{
    //How many objects in this pool
    uint32_t count;   

    //Free objects
    uint32_t nb_frees;
    // size in bytes of each object           
    uint32_t object_size;         

    //object cache data      
    char *cache_data;     

    //Free objects are linked by list.
    struct object_node* free_node_array;     
};

uint64_t pool_header_size(uint32_t count);
void pool_header_init(struct object_cache_pool *pool, uint32_t count,uint32_t object_size,
                      uint64_t header_size,uint8_t *object_data);
                      
void* pool_get(struct object_cache_pool *pool);
bool pool_release(struct object_cache_pool *pool, void* object);

#endif