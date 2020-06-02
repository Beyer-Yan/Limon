
#include <assert.h>
#include "io.h"
#include "slab.h"
#include "kverrno.h"
#include "hashmap.h"

#include "spdk/thread.h"

static void 
_dummy_blob_process_async(void*ctx){
    void **data = ctx;
    spdk_blob_op_complete cb_fn = data[0];
    void *cb_arg = data[1];
    free(data);
    cb_fn(cb_arg,0);
}

static void
_dummy_blob_write(struct spdk_blob *blob, struct spdk_io_channel *channel,
		   void *payload, uint64_t offset, uint64_t length,
		   spdk_blob_op_complete cb_fn, void *cb_arg){

    spdk_blob_io_write(blob,channel,payload,offset,length,cb_fn,cb_arg);
}

static void _store_pages_complete_cb(void*ctx, int kverrno);

static void
_process_cache_io(struct cache_io *cio,int kverrno){
    cio->cnt++;
    cio->kverrno = kverrno ? kverrno : -KV_ESUCCESS;

    if(cio->cnt==cio->nb_segments){
        //All the segments completes.
        cio->cb(cio->ctx,cio->kverrno);
        
        //this cio shall be lastly released!!
        pool_release(cio->imgr->cache_io_pool,cio);
    }
}

static void
_store_pages_multipages_phase2(struct page_io *pio){

    _dummy_blob_write(pio->blob,pio->imgr->channel,
                        pio->buf,pio->start_page,1,
                        _store_pages_complete_cb,pio);
}

static void
_store_pages_complete_cb(void*ctx, int kverrno){
    struct page_io *pio = ctx;

    pio->imgr->nb_pending_io--;

    _process_cache_io(pio->cache_io,kverrno);
    if(pio->io_link){
        _store_pages_multipages_phase2(pio->io_link);
    }

    pool_release(pio->imgr->page_io_pool,pio);
}

static void
_store_pages_multipages(struct iomgr* imgr,struct spdk_blob* blob,
                        uint64_t key_prefix,uint8_t* buf,uint64_t start_page,uint64_t nb_pages, 
                        struct cache_io *cio){
    
    //Split pages into page(0,n-2) and page(n-1).
    //Perform two-phase writing.          
    struct page_io* pio_phase1 = pool_get(imgr->page_io_pool);
    assert(pio_phase1!=NULL);

    pio_phase1->cache_io = cio;
    pio_phase1->key      = key_prefix;
    pio_phase1->imgr     = imgr;
    pio_phase1->len      = nb_pages - 1;

    struct page_io* pio_phase2 = pool_get(imgr->page_io_pool);
    assert(pio_phase2!=NULL);

    pio_phase2->cache_io   = cio;
    pio_phase2->key        = key_prefix + nb_pages - 1;
    pio_phase2->imgr       = imgr;
    pio_phase2->blob       = blob;
    pio_phase2->start_page = start_page + nb_pages - 1;
    pio_phase2->len        = 1;
    pio_phase2->buf        = buf + KVS_PAGE_SIZE * (nb_pages - 1);
    pio_phase2->io_link    = NULL;

    pio_phase1->io_link = pio_phase2;

    //Perform phase1 writing.
    imgr->nb_pending_io++;
    _dummy_blob_write(blob,imgr->channel,buf,start_page,pio_phase1->len,
                        _store_pages_complete_cb,pio_phase1);
}

static void 
_store_pages_one_page(struct iomgr* imgr,struct spdk_blob* blob,
                      uint64_t key_prefix,uint8_t* buf,uint64_t start_page, 
                      struct cache_io *cio){

    struct page_io* pio = pool_get(imgr->page_io_pool);
    assert(pio!=NULL);

    pio->cache_io = cio;
    pio->key = key_prefix;
    pio->imgr = imgr;
    pio->io_link = NULL;

    //Now issue a blob IO command for pio_1_pages;
    imgr->nb_pending_io++;
    _dummy_blob_write(blob,imgr->channel,buf,start_page,1,
                        _store_pages_complete_cb,pio);
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
    cio->cnt=0;

    //_make_cache_key128(key_prefix,nb_pages,cio->key);

    cio->cnt = 0;

    if(nb_pages==1){
        cio->nb_segments = 1;
        _store_pages_one_page(imgr,blob,key_prefix,buf,start_page,cio);
    }
    else{
        cio->nb_segments = 2;
        //Two-phases writing.
        _store_pages_multipages(imgr,blob,key_prefix,buf,start_page,nb_pages,cio);
    }
}
