#include <assert.h>
#include "iomgr.h"
#include "slab.h"
#include "kverrno.h"

static void
_process_cache_io(struct cache_io *cio, int kverrno){
    cio->cnt++;
    cio->kverrno = kverrno ? kverrno : -KV_ESUCCESS;

    if(cio->cnt==cio->nb_segments){
        //All the segments completes.
        struct cache_io *i=NULL, *tmp=NULL;
        TAILQ_FOREACH_SAFE(i,&cio->cio_head,link,tmp){
            TAILQ_REMOVE(&cio->cio_head,i,link);
            i->cb(i->ctx,cio->kverrno);
            pool_release(cio->imgr->cache_io_pool,i);
        }
        cio->cb(cio->ctx,cio->kverrno);
        HASH_DEL(cio->imgr->read_hash.cache_hash,cio); 
    }
}

static void
_process_io_link(struct page_io *pio, int kverrno){
    struct page_io *i=NULL, *tmp=NULL;
    TAILQ_FOREACH_SAFE(i,&pio->pio_head,link,tmp){
        TAILQ_REMOVE(&pio->pio_head,i,link);
        _process_cache_io(i->cache_io,kverrno);
        pool_release(pio->imgr->page_io_pool,i);
    }
    _process_cache_io(pio->cache_io,kverrno);
    pool_release(pio->imgr->page_io_pool,pio);
    HASH_DEL(pio->imgr->read_hash.page_hash,pio);
}

static void 
_default_page_io_complete_cb(void*ctx, int kverrno){
    struct page_io *pio = ctx;
    struct page_io *io_link = pio->io_link;

    pio->imgr->nb_pending_io--;

    struct page_io *i=NULL, *tmp=NULL;
    TAILQ_FOREACH_SAFE(i,&pio->pio_head,link,tmp){
        TAILQ_REMOVE(&pio->pio_head,i,link);
        _process_cache_io(i->cache_io,kverrno);
        pool_release(pio->imgr->page_io_pool,i);
    }
    _process_cache_io(pio->cache_io,kverrno);
    pool_release(pio->imgr->page_io_pool,pio);
    HASH_DEL(pio->imgr->read_hash.page_hash,pio);

    if(io_link!=NULL){
        _process_io_link(io_link,kverrno);
    }
}

static void
_default_cache_io_complete_cb(void*ctx, int kverrno){
    struct cache_io *cio = ctx;
    cio->imgr->nb_pending_io--;
    _process_cache_io(cio,kverrno);
}

static void
_load_pages_multipages(struct iomgr* imgr,struct slab*slab,uint64_t key_prefix,uint8_t* buf,uint64_t start_page,
                       uint64_t nb_pages, struct cache_io *cio){

    struct page_io *pio_1,*pio_n;
    struct page_io *tmp_1, *tmp_n;
    uint64_t page_1_key, page_n_key;

    page_1_key = key_prefix;
    page_n_key = key_prefix + nb_pages-1;

    pio_1 = pool_get(imgr->page_io_pool);
    assert(pio_1!=NULL);
    pio_1->cache_io = cio;
    pio_1->key = page_1_key;
    pio_1->imgr = imgr;
    pio_1->io_link = NULL;
    HASH_FIND_64(imgr->read_hash.page_hash,&page_1_key,tmp_1);

    pio_n = pool_get(imgr->page_io_pool);
    assert(pio_n!=NULL);
    pio_n->cache_io = cio;
    pio_n->key = page_n_key;
    pio_n->imgr = imgr;
    pio_n->io_link = NULL;
    HASH_FIND_64(imgr->read_hash.page_hash,&page_n_key,tmp_n);

    //Both ends pages are being laoded.
    if(tmp_1!=NULL && tmp_n!=NULL){
        //All end pages are loading.
        TAILQ_INSERT_TAIL(&tmp_1->pio_head,pio_1,link);
        TAILQ_INSERT_TAIL(&tmp_n->pio_head,pio_n,link);
        if(nb_pages>2){
            //I need load page 2 to n-1
            cio->nb_segments++;
            imgr->nb_pending_io++;
            spdk_blob_io_read(slab->blob,imgr->channel,
                              buf+KVS_PAGE_SIZE,start_page+1,nb_pages-2,
                              _default_cache_io_complete_cb,cio);
        }
    }
    else if(tmp_1==NULL && tmp_n==NULL){
        TAILQ_INIT(&pio_1->pio_head);
        TAILQ_INIT(&pio_n->pio_head);

        pio_1->io_link = pio_n;

        HASH_ADD_64(imgr->read_hash.page_hash,key,pio_1);
        HASH_ADD_64(imgr->read_hash.page_hash,key,pio_n);

        imgr->nb_pending_io++;
        spdk_blob_io_read(slab->blob,imgr->channel,
                              buf,start_page,nb_pages,
                              _default_page_io_complete_cb,cio);
    }
    else{
        if(tmp_1!=NULL){
            start_page++;
            buf = buf + KVS_PAGE_SIZE;
            TAILQ_INSERT_TAIL(&tmp_1->pio_head,pio_1,link);
            TAILQ_INIT(&pio_n->pio_head);
            HASH_ADD_64(imgr->read_hash.page_hash,key,pio_n);
        }
        else{
            TAILQ_INSERT_TAIL(&tmp_n->pio_head,pio_n,link);
            TAILQ_INIT(&pio_1->pio_head);
            HASH_ADD_64(imgr->read_hash.page_hash,key,pio_1);   
        }
        imgr->nb_pending_io++;
        spdk_blob_io_read(slab->blob,imgr->channel,
                              buf,start_page,nb_pages-1,
                              _default_page_io_complete_cb,cio);
    }
}

static void 
_load_pages_one_page(struct iomgr* imgr,struct slab*slab,uint64_t key_prefix,uint8_t* buf,uint64_t start_page, 
                     struct cache_io *cio){
    struct page_io* pio = NULL;
    struct page_io* tmp = NULL;

    pio = pool_get(imgr->page_io_pool);
    assert(pio!=NULL);
    pio->cache_io = cio;
    pio->key = key_prefix;
    pio->imgr = imgr;
    pio->io_link = NULL;
    HASH_FIND_64(imgr->read_hash.page_hash,&key_prefix,tmp);

    if(tmp!=NULL){
        //Someone else is already loading this page.
        TAILQ_INSERT_TAIL(&tmp->pio_head,pio,link);
    }
    else{
        TAILQ_INIT(&pio->pio_head);
        HASH_ADD_64(imgr->read_hash.page_hash,key,pio);
        //Now issue a slab IO command for pio_1_pages;
        imgr->nb_pending_io++;
        spdk_blob_io_read(slab->blob,imgr->channel,
                              buf,start_page,1,
                              _default_page_io_complete_cb,pio);
    }
}

void 
iomgr_load_pages_async(struct iomgr* imgr,struct slab*slab,uint64_t key_prefix,  
                       uint8_t* buf,uint64_t start_page, uint64_t nb_pages, 
                       void(*cb)(void*ctx, int kverrno), 
                       void* ctx){

    assert( ((uint64_t)buf) % KVS_PAGE_SIZE==0 );

    struct cache_io *cio = NULL, *tmp = NULL;

    cio = pool_get(imgr->cache_io_pool);
    assert(cio!=NULL);
    cio->cb = cb;
    cio->ctx = ctx;
    cio->imgr = imgr;
    _make_cache_key128(key_prefix,nb_pages,cio->key);

    HASH_FIND(hh,imgr->read_hash.cache_hash,cio->key,sizeof(cio->key),tmp);
    if(tmp!=NULL){
        //Other IOs are already prefetching the same pages!
        TAILQ_INSERT_TAIL(&tmp->cio_head,cio,link);
        return;
    }
    cio->cnt = 0;
    HASH_ADD(hh,imgr->read_hash.cache_hash,key,sizeof(cio->key),cio);

    if(nb_pages==1){
        cio->nb_segments = 1;
        _load_pages_one_page(imgr,slab,key_prefix,buf,start_page,cio);
    }
    else{
        //Two-phase loading.
        cio->nb_segments = 2;
        _load_pages_multipages(imgr,slab,key_prefix,buf,start_page,nb_pages,cio);
    }
}
