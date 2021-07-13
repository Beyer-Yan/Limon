#ifndef KVS_INDEX_H
#define KVS_INDEX_H

#include <stdint.h>
#include <assert.h>
#include "item.h"

/**
 * @brief When the item is put, the writing flag shall be set.
 * When the item is deleted, the deleting flag shell be set.
 * 
 * When an operation finishes, the flag will be cleared.
 * 
 * Get      --> non-flag
 * Put      --> writing
 * Delete   --> deleting
 * 
 * Operation Compatibility table 
 * 
 *             Get    Put    Delete
 *  Get        Y      A      Y     
 *  Put        N      N      N     
 *  Delete     N      N      N     
 * 
 * When 'N' compatibility is detected, the request will be resubmited
 * until the previous operation finishes. A means the compatibility is
 * checked accordingly.
 * 
 * The scan is a sychronized operation, and it will skip the entry marked as Deleting. 
 */ 
struct index_entry {
    uint64_t getting :1;
    uint64_t writing :1;
    uint64_t deleting:1;
    uint64_t shard   :10;
    uint64_t slab    :10;
    uint64_t slot_idx:41;
    //struct chunk_desc * chunk_desc;
};

static_assert(sizeof(struct index_entry)==8,"size incorrect");

struct mem_index;

typedef int (*mem_cb)(void *ctx, const uint8_t *key, uint32_t key_len, void *value);

/**
 * @brief Allocate a new mem index run time object.
 * 
 * @return struct mem_index*  The newly allocated mem inex object. NULL if allocations fails.
 */
struct mem_index * mem_index_init(void);

void mem_index_destroy(struct mem_index* mem_index);

/**
 * @brief Add the item into memory index. The entry will be copied into the index. You
 * have to free the entry memory after calling the function.
 * 
 * @param mem_index  The memory index.
 * @param item       The key of item to be added.
 * @param entry      The value to be added.
 * @return void*     NULL:Add failed beacause of either OOM ,or already-existence of the entry
 * not NULL:the added entry pointer of in memory index.
 */
void* mem_index_add(struct mem_index *mem_index, struct kv_item *item, struct index_entry* entry);

/**
 * @brief Deletes a item index from the memory index
 * 
 * @param mem_index  The memory index.
 * @param item       The key of item to be deleted.
 * 
 */
void mem_index_delete(struct mem_index *mem_index,struct kv_item *item);

/**
 * @brief Look up the entry for the given item.
 * 
 * @param mem_index             The in-mem data structure
 * @param item                  The given item
 * @return struct index_entry*  The entry if finding, else NULL.
 */
struct index_entry* mem_index_lookup(struct mem_index *mem_index, struct kv_item *item);

/**
 * @brief  iterate the mem index orderly. If the base_item is specified, the iterating
 * will go though all item with keys greater than base_item. If the base_item is NULL,
 * the iterating will go though from start. 
 * The call back gets a key, value for each and returns an integer stop value.
 * If the callback returns non-zero, then the iteration stops.
 * 
 * @param mem_index         The in-mem data.
 * @param base_item         The base item.
 * @param cb_fn             callback for each item.
 * @param ctx               user callback context.
 * @return int              0 on success, or the return of the callback.
 */
int mem_index_iter(struct mem_index *mem_index,struct kv_item *base_item,mem_cb cb_fn, void*ctx);

#endif

