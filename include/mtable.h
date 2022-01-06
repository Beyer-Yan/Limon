#ifndef KVS_MTABLE_H
#define KVS_MTABLE_H

#include <stdint.h>

struct slot_entry {
    union
    {
        uint64_t raw;
        struct{
            uint64_t getting :1;
            uint64_t putting :1;
            uint64_t deleting:1;
            uint64_t shard   :10;
            uint64_t slab    :10;
            uint64_t slot_idx:41;
        };
    };
};

static_assert(sizeof(struct slot_entry)==8,"incorrect size");

struct mtable;

/**
 * @brief Get worker id from the sid.
 * 
 * @param sid        the given sid.
 * @return uint32_t  the returned worker id.
 */
uint32_t mtable_get_worker_from_sid(uint64_t sid);

/**
 * @brief Create a new mtable object
 * 
 * @param id The unique id of the mtable object
 * @param window_size the capacity of deleting window
 * @return struct mtable* The newly created mtable
 */
struct mtable* mtable_new(uint64_t id, uint32_t window_size);

/**
 * @brief Destroy the given mtable object
 * 
 * @param mt The given mtable object
 */
void mtable_destroy(struct mtable* mt);

/**
 * @brief Get the slot entry from the given 8-byte sid
 * 
 * @param mt   The mtable object
 * @param sid  The given sid
 * @return struct slot_entry*  The slot_entry pointer if the given sid exists,
 *                             otherwise NULL.
 */
struct slot_entry* mtable_get(struct mtable* mt,uint64_t sid);

/**
 * @brief Check whether the given sid is valid
 * 
 * @param mt     The mtable object
 * @param sid    The given sid
 * @return int   1 for valud, 0 for invalid.
 */
int mtable_check_valid(struct mtable* mt,uint64_t sid);

/**
 * @brief Allocate one slot_entry from the given mtable object and fill
 *        the entry data with entry.
 * 
 * @param mt         The given mtable object
 * @param entry      The 8-byte slot_entry
 * @return uint64_t  The new allocated sid. This operation always sucesses
 *                   except a OOM.
 */
uint64_t mtable_alloc_sid(struct mtable* mt, struct slot_entry new_entry);

/**
 * @brief     Release a sid of the mtable
 * 
 * @param mt   The given mtable object
 * @param sid  The sid to be released
 */
void mtable_release(struct mtable* mt,uint64_t sid);

#endif