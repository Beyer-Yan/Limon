#ifndef KVS_IO_H
#define KVS_IO_H

#include <stdint.h>
#include "queue.h"
#include "uthash.h"
#include "pool.h"

#include "spdk/blob.h"

#define BASE_DIVISOR 315

//typedef void (*pending_cb)(struct cache_io *cache_io);

struct page_io{
    uint64_t key;

    /**
     * @brief When performing the loading, the io_link is used to link the last
     * page io of a multi-pages loading if the last page is not in loading state. 
     * 
     * When performing the storing, the io_link is used to indicate whther current
     * page IO needs to do phase-2 writing. If it is NULL, I needn't do phawe-2
     * writing. Ortherwise, the phase-2 writing need performing.
     */
    struct page_io *io_link;

    struct cache_io *cache_io;
    struct iomgr *imgr;

    //For two-phase writing only.
    uint32_t len;

    TAILQ_HEAD(, page_io) pio_head;
    TAILQ_ENTRY(page_io) link;
    UT_hash_handle hh;
};

struct cache_io{
    uint64_t key[2];
    struct iomgr *imgr;

    //For loading only
    uint32_t cnt;
    uint32_t nb_segments;

    int kverrno;
    void(*cb)(void*ctx, int kverrno);
    void* ctx;

    //For two-phase writing only.
    uint64_t start_page;
    uint64_t nb_pages;
    struct spdk_blob* blob;
    uint64_t key_prefix;
    uint8_t *buf;

    TAILQ_HEAD(, cache_io) cio_head;
    TAILQ_ENTRY(cache_io) link;
    UT_hash_handle hh;
};

struct pending_io_hash{
    struct page_io *page_hash;
    struct cache_io* cache_hash;
};

struct iomgr{
    struct spdk_io_channel *channel;
    uint32_t max_pending_io; 
    uint32_t nb_pending_io;
    struct pending_io_hash read_hash;
    struct pending_io_hash write_hash;
    struct object_cache_pool *cache_io_pool;
    struct object_cache_pool *page_io_pool;
};

static inline void 
_make_cache_key128(uint64_t base_key, uint64_t n, uint64_t *key_out){

    key_out[1] = base_key;
    key_out[0] = n;
}

static inline uint64_t 
_make_page_key64(uint64_t base_key,uint64_t off){
    return base_key + off;
}

/**
 * @brief Issue an io command to load data pages from blob file.
 * 
 * @param imgr       The io manager.
 * @param blob       The blob file.
 * @param key_prefix The key prefix of the io. IOs with the same key will be mergered.
 * @param buf        The data buffer, must be 4KB aligned.
 * @param start_page The start page of the data in blob file.
 * @param nb_pages   The number of pages.
 * @param cb         User callback
 * @param ctx        Parameter of user callback.
 */
void iomgr_load_pages_async(struct iomgr* imgr,
                            struct spdk_blob* blob,
                            uint64_t key_prefix, 
                            uint8_t* buf,
                            uint64_t start_page, 
                            uint64_t nb_pages, 
                            void(*cb)(void*ctx, int kverrno), 
                            void* ctx);

/**
 * @brief Issue an io command to store data pages to blob file.
 * 
 * @param imgr       The io manager.
 * @param blob       The blob file.
 * @param key_prefix The key prefix of the io. IOs with the same keys will be mergered.
 * @param buf        The data buffer, must be 4KB aligned.
 * @param start_page The start page of the data in blob file.
 * @param nb_pages   The number of pages.
 * @param cb         User callback
 * @param ctx        Parameter of user callback.
 */
void iomgr_store_pages_async(struct iomgr* imgr,
                            struct spdk_blob* blob, 
                            uint64_t key_prefix, 
                            uint8_t* buf,
                            uint64_t start_page, 
                            uint64_t nb_pages,                             
                            void(*cb)(void*ctx, int kverrno), 
                            void* ctx);
#endif
