#ifndef __NDEX_H
#define __INDEX_H

#include <stdint.h>
#include "item.h"
#include "pagechunk.h"

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
 *  Get        Y      Y      Y     
 *  Put        N      N      N     
 *  Delete     N      N      N     
 * 
 * When 'N' compatibility is detected, the request will be resubmited
 * until the previous operation finishes.
 * 
 * The scan is a sychronized operation, and it will skip the entry marked as Deleting. 
 */ 
struct index_entry {
    uint64_t writing :1;
    uint64_t deleting:1;
    uint64_t slot_idx:62;
    struct chunk_desc * chunk_desc;
};

struct mem_index;

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
void* mem_index_add(struct mem_index *mem_index, struct kv_item *item,struct index_entry* entry);

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
 * @brief Get the first item.
 * 
 * @param mem_index     The in-mem data
 * @param item_out      The returned item. The item_out needn't releasing, since it is allocated
 * statically. Uers have to copy its value out, as it will be flushed every time when users call
 * this function.
 */
void mem_index_first(struct mem_index *mem_index,struct kv_item **item_out);

/**
 * @brief Get the first item, which is greater than the base item.
 * 
 * @param mem_index     The in-mem data
 * @param base_item     The base item.
 * @param item_out      Return the first item that is greater than the base item. 
 * The item_out needn't releasing, since it is allocated statically. Uers have to
 * copy its value out, as it will be flushed every time when users call this function.
 */
void mem_index_next(struct mem_index *mem_index,struct kv_item *base_item,struct kv_item **item_out);

#endif

