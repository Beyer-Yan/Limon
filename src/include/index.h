#ifndef KVS_INDEX_H
#define KVS_INDEX_H

#include <stdint.h>
#include <assert.h>
#include "art_helpler.h"
#include "item.h"

struct mem_index;

/**
 * @brief Allocate a new mem index run time object.
 * 
 * @return struct mem_index*  The newly allocated mem inex object. NULL if allocations fails.
 */
static inline struct mem_index * mem_index_init(void){
    return (struct mem_index*)art_new();
}

static inline void mem_index_destroy(struct mem_index* mem_index){
    assert(mem_index);
    struct art *t = (struct art*)mem_index;
    art_destroy(t);
}

/**
 * @brief Add the item into memory index. The entry will be copied into the index. You
 * have to free the entry memory after calling the function.
 * 
 * @param mem_index  The memory index.
 * @param item       The key of item to be added.
 * @param sid        The 8-byte slot id
 * @return int       0 sucess, others for failure
 */
static inline int mem_index_add(struct mem_index *mem_index, struct kv_item *item, uint64_t sid){
    assert(mem_index);
    assert(item);
    assert(sid);
    assert(item->meta.ksize==8 && "Not supported for key size larger than 8");

    uint64_t key = ((uint64_t*)(item->data))[0];
    struct art *t = (struct art*)mem_index;

    return art_put(t,key,sid);
}

/**
 * @brief Deletes a item index from the memory index
 * 
 * @param mem_index  The memory index.
 * @param item       The key of item to be deleted.
 * 
 */
static inline void mem_index_delete(struct mem_index *mem_index,struct kv_item *item){
    assert(mem_index);
    assert(item); 
    assert(item->meta.ksize==8 && "Not supported for key size larger than 8");

    uint64_t key = ((uint64_t*)(item->data))[0];
    struct art *t = (struct art*)mem_index;

    art_del(t,key);       
}

/**
 * @brief Look up the entry for the given item.
 * 
 * @param mem_index   The in-mem data structure
 * @param item        The given item
 * @return uint64_t   The sid if finding, else 0.
 */
static inline uint64_t mem_index_lookup(struct mem_index *mem_index, struct kv_item *item){
    assert(mem_index);
    assert(item); 
    assert(item->meta.ksize==8 && "Not supported for key size larger than 8");

    uint64_t key = ((uint64_t*)(item->data))[0];
    struct art *t = (struct art*)mem_index;    

    return art_get(t,key);
}

/**
 * @brief scan from the given key
 * 
 * @param mem_index   The index handle
 * @param start_key   The start item
 * @param maxLen      The max scan length
 * @param founds      The actual scaned items
 * @param sid_array   The result sid array
 */
static inline void mem_index_scan(struct mem_index *mem_index,struct kv_item *item, int maxLen, int* founds, uint64_t *sid_array){
    assert(mem_index);
    assert(item); 
    assert(founds);
    assert(sid_array);
    assert(item->meta.ksize==8 && "Not supported for key size larger than 8");

    uint64_t key = ((uint64_t*)(item->data))[0];
    struct art *t = (struct art*)mem_index;    

    art_scan(t,key,maxLen,founds,sid_array);  
}

#endif

