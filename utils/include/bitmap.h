#ifndef KVS_BITMAP_H
#define KVS_BITMAP_H

#include <stdint.h>
#include <assert.h>

struct bitmap {
    uint32_t length;
    uint64_t  data[0];
};

static_assert(sizeof(struct bitmap)==8,"incorrect size");

/**
 * @brief Get the length of the given bitmap
 * 
 * @param map        the given bitmap object
 * @return uint32_t  the length og the given bitmap
 */
uint32_t bitmap_get_length(struct bitmap *map);

/**
 * @brief Get the total size of the given bitmap
 * 
 * @param num_bits  the count of bits
 * @return uint32_t the total size
 */
uint32_t bitmap_header_size(uint32_t num_bits);

/**
 * @brief Get the bit indicated by id
 * 
 * @param map  the given bitmap object
 * @param id   the given id
 * @return uint32_t  the bit status, 0 or 1
 */
uint32_t bitmap_get_bit(struct bitmap *map ,uint32_t id);

/**
 * @brief Set 1 for the given bitmap of position id
 * 
 * @param map the given bitmap object
 * @param id  the bit position
 */
void bitmap_set_bit(struct bitmap *map,uint32_t id);

/**
 * @brief Get the first clear bit from the given bitmap
 * 
 * @param map  the given bitmap
 * @return uint32_t the clear bit position
 */
uint32_t bitmap_get_first_clear_bit(struct bitmap *map);

/**
 * @brief Set 1 for the bits range from [lo,hi]
 * 
 * @param map the given bitmap object
 * @param lo  the given bitmap object
 * @param hi  the given bitmap object
 */
void bitmap_set_bit_range(struct bitmap *map,uint32_t lo, uint32_t hi);

/**
 * @brief Clear the given bit of the given bitmap object
 * 
 * @param map  the given bitmap object
 * @param id   the bit position
 */
void bitmap_clear_bit(struct bitmap *map,uint32_t id);

/**
 * @brief Clear(Set to 0) bit range [lo,hi]
 * 
 * @param map the given bitmap object
 * @param lo  the given bitmap object
 * @param hi  the given bitmap object
 */
void bitmap_clear_bit_range(struct bitmap *map,uint32_t lo, uint32_t hi);

/**
 * @brief Clear all bits of the given bitmap
 * 
 * @param map the given bitmap object
 */
void bitmap_clear_bit_all(struct bitmap *map);

#endif