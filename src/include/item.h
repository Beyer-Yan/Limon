#ifndef KVS_ITEM_H
#define KVS_ITEM_H

#include <stdint.h>
#include <assert.h>

#define MAX_ITEM_SIZE 64*1024

// data struct in persistent storage
struct item_meta {
    uint8_t cdt[8]; //create timestamp
    uint8_t dlt[8]; //delete timestmap
    uint32_t ksize;
    uint32_t vsize;
};

// data item in memory
struct kv_item {
    struct item_meta meta;
    //0 size array is not the standard of c.
    uint8_t data[0];
};

static_assert(sizeof(struct item_meta)==24,"incorrect size");
static_assert(sizeof(struct kv_item)==sizeof(struct item_meta),"incorrect size");

// meta size + key size + value size
static inline uint32_t item_get_size(struct kv_item *item){
    return item->meta.ksize + item->meta.vsize + sizeof(struct item_meta);
}

//8 bytes prefix-rdt + 8 bytes postfix-rdt
static inline uint32_t item_packed_size(struct kv_item *item){
    return item_get_size(item) + 16;
}

#endif
