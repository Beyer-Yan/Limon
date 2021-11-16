#ifndef KVS_IO_H
#define KVS_IO_H

#include <stdint.h>
#include "spdk/queue.h"
#include "pool.h"
#include "hashmap.h"

#include "spdk/blob.h"

#define BASE_DIVISOR 315

//typedef void (*pending_cb)(struct cache_io *cache_io);

struct page_io{
    uint64_t key;

    /**
     * @brief When performing the loading, the io_link is used to link the last
     * page io of a multi-pages loading if the last page is not in loading state. 
     * 
     * When performing the storing, the io_link is used to indicate whether current
     * page IO needs to do phase-2 writing. If it is NULL, I needn't do phase-2
     * writing. Ortherwise, the phase-2 writing need performing.
     */
    struct page_io *io_link;

    struct cache_io *cache_io;
    struct iomgr *imgr;

    uint64_t start_page;
    uint64_t len;
    struct spdk_blob* blob;
    uint8_t *buf;

    TAILQ_HEAD(, page_io) pio_head;
    TAILQ_ENTRY(page_io) link;
};

struct cache_io{
    uint64_t key;
    struct iomgr *imgr;

    //nb_segments means how many page_ios for the cache_io
    uint32_t cnt;
    uint32_t nb_segments;
    uint32_t nb_pages;
    uint64_t start_page;
    uint8_t* buf;
    struct spdk_blob *blob;

    int kverrno;
    void(*cb)(void*ctx, int kverrno);
    void* ctx;

    TAILQ_HEAD(, cache_io) cio_head;
    TAILQ_ENTRY(cache_io) link;
};

struct pending_io_hash{
    map_t page_hash;
    map_t cache_hash;
};

struct iomgr{
    struct spdk_io_channel *channel;
    //When I resize blob, I shall send such operation to the 
    //the thread that initializing the blobstore.
    struct spdk_thread *meta_thread;
    struct spdk_blob_store *target;
    
    uint32_t max_pending_io; 
    uint32_t nb_pending_io;

    uint64_t io_unit_size;

    TAILQ_HEAD(,cache_io) pending_read_head;
    TAILQ_HEAD(,cache_io) pending_write_head;

    struct pending_io_hash read_hash;
    struct pending_io_hash write_hash;

    struct object_cache_pool *cache_io_pool;
    struct object_cache_pool *page_io_pool;

    //for stats;
    uint64_t page_reads[100];
    uint64_t page_writes[100];

};

static inline uint64_t 
_make_cache_key(uint64_t base_key, uint64_t n){
    //We reserve the most significant 16 bytes.
    //This the most significant 16 bytes should not
    //be used. Otherwise the keys may not uniquely identify
    //the pages.
    assert(n<UINT16_MAX);
    return (base_key<<16)+n;
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

/**
 * @brief Poll the pending write requests.
 * 
 * @param imgr  The iomgr
 * @return int  The number of requests processed.
 */
int iomgr_io_write_poll(struct iomgr* imgr);

/**
 * @brief Poll all the pending requests.
 * 
 * @param imgr  The iomgr
 * @return int  The number of requests processed.
 */
static inline int iomgr_io_poll(struct iomgr* imgr){
    int events = 0;
    events += iomgr_io_write_poll(imgr);

    return events;
}

#endif
