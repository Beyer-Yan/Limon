
#include <assert.h>
#include "io.h"
#include "slab.h"
#include "kverrno.h"
#include "hashmap.h"

#include "spdk/thread.h"
#include "spdk/log.h"

static inline void _write_pages(struct iomgr* imgr, struct spdk_blob* blob,
		       void *payload, uint64_t offset, uint64_t length,
		       spdk_blob_op_complete cb_fn, void *cb_arg){

    uint64_t io_unit_size = imgr->io_unit_size;
    struct spdk_io_channel *channel = imgr->channel;

    imgr->page_writes[length]++;

    uint64_t io_unit_per_page = KVS_PAGE_SIZE/io_unit_size;
    uint64_t io_unit_offset = offset*io_unit_per_page;
    uint64_t io_uint_length = length*io_unit_per_page;

    spdk_blob_io_write(blob,channel,payload,io_unit_offset,io_uint_length,cb_fn,cb_arg);
}

static void _store_pages_complete_cb(void*ctx, int kverrno);

static void
_process_cache_io(struct cache_io *cio,int kverrno){
    cio->cnt++;
    cio->kverrno = kverrno ? kverrno : -KV_ESUCCESS;

    if(cio->cnt==cio->nb_segments){
        //All the segments completes.
        cio->cb(cio->ctx,cio->kverrno);
        
        struct cache_io *i=NULL, *tmp=NULL;
        TAILQ_FOREACH_SAFE(i,&cio->cio_head,link,tmp){
            TAILQ_REMOVE(&cio->cio_head,i,link);

            //Release the pool first, then call the user callback.
            //the memory for cache_io i will not be freed. So I can 
            //still get the data.
            //In case that user call another put or get, the cache io will be
            //consumped reccurcively, inducing a pool-resource-not-enough error.
            //So I have to release the cache io firstly. 
            pool_release(cio->imgr->cache_io_pool,i);
            i->cb(i->ctx,cio->kverrno);
        }
        //this cio shall be lastly released!!
        pool_release(cio->imgr->cache_io_pool,cio);
    }
}

static void
_store_pages_phase2(struct page_io *pio){
    pio->imgr->nb_pending_io++;
    //SPDK_NOTICELOG("\tStoring pages2, buf:%p, off:%lu,nb_pages:%lu\n",pio->buf,pio->start_page,pio->len);
    _write_pages(pio->imgr,pio->blob,
                 pio->buf,pio->start_page,pio->len,
                 _store_pages_complete_cb,pio);
}

static void
_store_pages_complete_cb(void*ctx, int kverrno){
    struct page_io *pio = ctx;

    pio->imgr->nb_pending_io--;

    _process_cache_io(pio->cache_io,kverrno);
    if(pio->io_link){
        _store_pages_phase2(pio->io_link);
    }

    pool_release(pio->imgr->page_io_pool,pio);
}

void 
iomgr_store_pages_async(struct iomgr* imgr,
                            struct spdk_blob* blob, 
                            uint64_t key_prefix, 
                            uint8_t* buf,
                            uint64_t start_page, 
                            uint64_t nb_pages,                         
                            void(*cb)(void*ctx, int kverrno), 
                            void* ctx){

    assert( ((uint64_t)buf) % KVS_PAGE_SIZE==0 );

    struct cache_io *cio = NULL, *tmp = NULL;

    cio = pool_get(imgr->cache_io_pool);
    assert(cio!=NULL);
    cio->cb = cb;
    cio->ctx = ctx;
    cio->imgr = imgr;
    cio->start_page = start_page;
    cio->buf = buf;
    cio->nb_pages = nb_pages;
    cio->cnt=0;
    //SSDs often have 4096 byte blocks, but claim to have 512 byte blocks for compatibility
    //@wanring Ensure the page size is 512
    assert(KVS_PAGE_SIZE==512);
    cio->nb_segments = nb_pages>8 ? 2 : 1;
    cio->blob = blob;
    cio->key = _make_cache_key(key_prefix,nb_pages);

    //submit cio
    TAILQ_INSERT_TAIL(&imgr->pending_write_head,cio,link);
}

int iomgr_io_write_poll(struct iomgr* imgr){
    int events = 0;
    //Process the requests that covering the same pages
    map_t cmap = imgr->write_hash.cache_hash;
    struct cache_io *cio, *ctmp=NULL;
    TAILQ_FOREACH_SAFE(cio,&imgr->pending_write_head,link,ctmp){
        struct cache_io *val;
        hashmap_get(cmap,cio->key,&val);
        if(val){
            //They are writing the same pages, just link them.
            TAILQ_REMOVE(&imgr->pending_write_head,cio,link);
            TAILQ_INSERT_TAIL(&val->cio_head,cio,link);
        }
        else{
            TAILQ_INIT(&cio->cio_head);
            hashmap_put(cmap,cio->key,cio);
        }
    }

    TAILQ_HEAD(,page_io) pio_head;
    TAILQ_INIT(&pio_head);

    uint64_t del_tmp;
    //Process the requests that have interleaved pages
    TAILQ_FOREACH_SAFE(cio,&imgr->pending_write_head,link,ctmp){
        TAILQ_REMOVE(&imgr->pending_write_head,cio,link);
        hashmap_remove(cmap,cio->key,&del_tmp);

        struct page_io *pio1 = pool_get(imgr->page_io_pool);
        assert(pio1!=NULL);
        pio1->cache_io = cio;
        pio1->key = cio->key-1;
        pio1->imgr = imgr;
        pio1->io_link = NULL;
        pio1->buf = cio->buf;
        pio1->start_page = cio->start_page;
        pio1->len = cio->nb_segments==1 ? cio->nb_pages : cio->nb_pages - 1;
        pio1->blob = cio->blob;

        if(cio->nb_segments==2){
            //Perform phase2 writing.
            struct page_io *pio2 = pool_get(imgr->page_io_pool);
            assert(pio2!=NULL);
            uint32_t pages = cio->nb_pages - 1;
            pio2->cache_io = cio;
            pio2->key = cio->key;
            pio2->imgr = imgr;
            pio2->io_link = NULL;
            pio2->buf = cio->buf + KVS_PAGE_SIZE*pages;
            pio2->start_page = cio->start_page + pages;
            pio2->len = 1;
            pio2->blob = cio->blob;
            
            //pio2 shall be processed after pio1
            pio1->io_link = pio2;
        }
        TAILQ_INSERT_TAIL(&pio_head,pio1,link);
    }

    struct page_io *pio, *ptmp = NULL;
    TAILQ_FOREACH_SAFE(pio,&pio_head,link,ptmp){
        TAILQ_REMOVE(&pio_head,pio,link);
        events++;
        imgr->nb_pending_io++;
        //SPDK_NOTICELOG("\tStoring pages1, buf:%p, off:%lu,nb_pages:%lu\n",pio->buf,pio->start_page,pio->len);
        _write_pages(pio->imgr,pio->blob,
                     pio->buf,pio->start_page,pio->len,
                     _store_pages_complete_cb,pio);
    }
    return events;
}
