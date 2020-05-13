#ifndef __ITEM_H
#define __ITEM_H

#include <stdint.h>

#define MAX_ITEM_SIZE 64*1024

// data struct in persistent storage
struct item_meta {
    uint64_t cdt; //create timestamp
    uint64_t dlt; //delete timestmap
    int32_t ksize;
    int32_t vsize;
};

// data item in memory
struct kv_item {
    struct item_meta meta;
    unsigned char *data;
};

// meta size + key size + value size
inline uint32_t item_get_size(struct kv_item *item){
    return item->meta.ksize + item->meta.vsize + sizeof(struct item_meta);
}

//8 bytes prefix-rdt + 8 bytes postfix-rdt
inline uint32_t item_packed_size(struct kv_item *item){
    return item_get_size(item) + 16;
}

#endif
