#ifndef KVS_BITMAP_H
#define KVS_BITMAP_H

#include <stdint.h>
#include <assert.h>

struct bitmap {
    uint32_t length;
    uint8_t  data[0];
};

static_assert(sizeof(struct bitmap)==4,"incorrect size");

uint32_t bitmap_get_bit(struct bitmap *map ,uint32_t id);

void bitmap_set_bit(struct bitmap *map,uint32_t id);

uint32_t bitmap_get_first_clear_bit(struct bitmap *map);

void bitmap_set_bit_range(struct bitmap *map,uint32_t lo, uint32_t hi);

void bitmap_clear_bit(struct bitmap *map,uint32_t id);

void bitmap_clear_bit_range(struct bitmap *map,uint32_t lo, uint32_t hi);

void bitmap_clear_bit_all(struct bitmap *map);

#endif