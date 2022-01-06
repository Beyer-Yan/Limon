#ifndef KVS_POOL_H
#define KVS_POOL_H

#include <stdbool.h>
#include <stdint.h>

struct object_cache_pool{
    uint64_t key; 

    uint64_t count;  
    uint64_t object_size;  
    uint64_t nb_frees;        

    //object cache data      
    uint8_t *cache_data;     

    //Free objects are linked by list.
    uint64_t head;
    uint64_t free_node_array[0];     
};

/**
 * @brief Create new object pool
 * 
 * @param count        object count of the pool
 * @param object_size  object size
 * @return struct object_cache_pool* pool object if success, Otherwise NULL.
 */
struct object_cache_pool* pool_create(uint32_t count, uint32_t object_size);

/**
 * @brief Destroy the given pool
 * 
 * @param pool the given object pool
 */
void pool_destroy(struct object_cache_pool* pool);

/**
 * @brief Get an object from the given pool
 * 
 * @param pool   the given pool
 * @return void* the object handle if success, Otherwise NULL.
 */
void* pool_get(struct object_cache_pool *pool);

/**
 * @brief Release an object to the given pool
 * 
 * @param pool   the given object 
 * @param object the given object to be released 
 */
void pool_release(struct object_cache_pool *pool, void* object);

#endif
