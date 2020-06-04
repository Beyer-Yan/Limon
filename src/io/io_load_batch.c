#include <assert.h>
#include "io.h"
#include "slab.h"
#include "kverrno.h"
#include "hashmap.h"

#include "spdk/thread.h"


static void
_process_cache_io(struct cache_io *cio, int kverrno){
    cio->cnt++;
    cio->kverrno = kverrno ? kverrno : -KV_ESUCCESS;

    if(cio->cnt==cio->nb_segments){
        //All the segments completes.
        //process the header
        cio->cb(cio->ctx,cio->kverrno);

        //process the link requests
        struct cache_io *i=NULL, *tmp=NULL;
        TAILQ_FOREACH_SAFE(i,&cio->cio_head,link,tmp){
            TAILQ_REMOVE(&cio->cio_head,i,link);
            pool_release(cio->imgr->cache_io_pool,i);
            i->cb(i->ctx,cio->kverrno);
        }
        //This cio shall be lasted released!!
        //this cache io is finished, now remove it from cache read_hash
        pool_release(cio->imgr->cache_io_pool,cio);
    }
}

static void
_process_io_link(struct page_io *pio, int kverrno){
    //process the header
    
    _process_cache_io(pio->cache_io,kverrno);
    
    //process the linked requests.
    struct page_io *i=NULL, *tmp=NULL;
    TAILQ_FOREACH_SAFE(i,&pio->pio_head,link,tmp){
        TAILQ_REMOVE(&pio->pio_head,i,link);
        pool_release(pio->imgr->page_io_pool,i);
        _process_cache_io(i->cache_io,kverrno);
    }

    //This page io is finished, now remove it from the read_hash
    pool_release(pio->imgr->page_io_pool,pio);
}

static void 
_default_page_io_complete_cb(void*ctx, int kverrno){
    struct page_io *pio = ctx;
    struct page_io *io_link = pio->io_link;

    pio->imgr->nb_pending_io--;

    //process the header
    _process_cache_io(pio->cache_io,kverrno);
    
    if(io_link!=NULL){
        _process_io_link(io_link,kverrno);
    }
    //process the linked page io requests
    struct page_io *i=NULL, *tmp=NULL;
    TAILQ_FOREACH_SAFE(i,&pio->pio_head,link,tmp){
        TAILQ_REMOVE(&pio->pio_head,i,link);
        pool_release(pio->imgr->page_io_pool,i);
        _process_cache_io(i->cache_io,kverrno);
    }

    //This page io is finished, now remove it from the read_hash
    pool_release(pio->imgr->page_io_pool,pio);
}

void 
iomgr_load_pages_async(struct iomgr* imgr,struct spdk_blob* blob,uint64_t key_prefix,  
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
    cio->start_page = start_page;
    cio->buf = buf;
    cio->nb_pages = nb_pages;
    cio->cnt=0;
    cio->nb_segments = nb_pages>1 ? 2 : 1;
    cio->blob = blob;
    _make_cache_key128(key_prefix,nb_pages,cio->key);

    TAILQ_INSERT_TAIL(&imgr->pending_read_head,cio,link);
}

TAILQ_HEAD(pio_head,page_io);

static void
_load_pages_one_page(map_t pmap, struct cache_io *cio, struct iomgr *imgr, struct pio_head *phead){
    struct page_io *pio_1, *pval_1;

    pio_1 = pool_get(imgr->page_io_pool);
    assert(pio_1!=NULL);
    pio_1->cache_io = cio;
    pio_1->key = cio->key[0];
    pio_1->imgr = imgr;
    pio_1->io_link = NULL;

    if(hashmap_get(pmap,&pio_1->key,sizeof(pio_1->key),&pval_1)){
        TAILQ_INSERT_TAIL(phead,pio_1,link);
    }
    else{
        TAILQ_INIT(&pio_1->pio_head);
        pio_1->blob = cio->blob;
        pio_1->buf = cio->buf;
        pio_1->start_page = cio->start_page;
        pio_1->len = 1;
        hashmap_put(pmap,&pio_1->key,sizeof(pio_1->key),pio_1);
    }
}

static void
_load_pages_multipages(map_t pmap, struct cache_io *cio, struct iomgr *imgr, struct pio_head *phead){

    struct page_io *pio_1,*pio_n;
    struct page_io *tmp_1, *tmp_n;
    uint64_t page_1_key, page_n_key;

    struct page_io* submitted_pio = NULL;

    page_1_key = cio->key[0];
    page_n_key = cio->key[0] + cio->nb_pages-1;

    pio_1 = pool_get(imgr->page_io_pool);
    assert(pio_1!=NULL);
    pio_1->cache_io = cio;
    pio_1->key = page_1_key;
    pio_1->imgr = imgr;
    pio_1->io_link = NULL;
    pio_1->blob = cio->blob;

    pio_n = pool_get(imgr->page_io_pool);
    assert(pio_n!=NULL);
    pio_n->cache_io = cio;
    pio_n->key = page_n_key;
    pio_n->imgr = imgr;
    pio_n->io_link = NULL;
    pio_n->blob = cio->blob;
    
    hashmap_get(pmap,(uint8_t*)&page_1_key, sizeof(page_1_key),&tmp_1);
    hashmap_get(pmap,(uint8_t*)&page_n_key, sizeof(page_n_key),&tmp_n);

    //Both ends pages are being loaded.
    if(tmp_1!=NULL && tmp_n!=NULL){
        //All end pages are loading.
        TAILQ_INSERT_TAIL(&tmp_1->pio_head,pio_1,link);
        TAILQ_INSERT_TAIL(&tmp_n->pio_head,pio_n,link);
        if(cio->nb_pages>2){
            //I need load page 2 to n-1
            cio->nb_segments++;
            struct page_io *pio_internal = pool_get(imgr->page_io_pool);
            assert(pio_internal!=NULL);

            //internal pages for cio must not be interleaved.
            pio_internal->cache_io = cio;
            pio_internal->key = page_1_key + 1;
            pio_internal->imgr = imgr;
            pio_internal->io_link = NULL;
            pio_internal->blob = cio->blob;
            pio_internal->buf = cio->buf + KVS_PAGE_SIZE;
            pio_internal->start_page = cio->start_page + 1;
            pio_internal->len = cio->nb_pages - 2;
            
            submitted_pio = pio_internal;
        }
    }
    else if(tmp_1==NULL && tmp_n==NULL){
        //Both two pages are not loaded
        TAILQ_INIT(&pio_1->pio_head);
        TAILQ_INIT(&pio_n->pio_head);

        pio_1->io_link = pio_n;

        hashmap_put(pmap,(uint8_t*)&pio_1->key, sizeof(page_1_key),pio_1);
        hashmap_put(pmap,(uint8_t*)&pio_n->key, sizeof(page_n_key),pio_n);
        
        submitted_pio = pio_1;
    }
    else{
        struct page_io* submitted_pio;
        if(tmp_1!=NULL){
            TAILQ_INSERT_TAIL(&tmp_1->pio_head,pio_1,link);
            TAILQ_INIT(&pio_n->pio_head);

            pio_n->buf = cio->buf + KVS_PAGE_SIZE;
            pio_n->start_page = cio->start_page + 1;
            pio_n->len = cio->nb_pages - 1;

            hashmap_put(pmap,(uint8_t*)&pio_n->key, sizeof(page_n_key),pio_n);
            submitted_pio = pio_n;
        }
        else{
            TAILQ_INSERT_TAIL(&tmp_n->pio_head,pio_n,link);
            TAILQ_INIT(&pio_1->pio_head);

            pio_1->buf = cio->buf;
            pio_1->start_page = cio->start_page;
            pio_1->len = cio->nb_pages - 1;

            hashmap_put(pmap,(uint8_t*)&pio_1->key, sizeof(page_1_key),pio_1); 
            submitted_pio = pio_1;
        }
    }

    if(submitted_pio){
        TAILQ_INSERT_TAIL(phead,submitted_pio,link);
    } 
}

int iomgr_io_read_poll(struct iomgr* imgr){
    int events = 0;
    //Process the requests that covering the same pages
    map_t cmap = hashmap_new();
    struct cache_io *cio, *ctmp=NULL;
    TAILQ_FOREACH_SAFE(cio,&imgr->pending_read_head,link,ctmp){
        struct cache_io *val;
        hashmap_get(cmap,&cio->key, sizeof(cio->key),&val);
        if(val){
            //They are writing the same pages, just link them.
            TAILQ_INSERT_TAIL(&val->cio_head,cio,link);
            TAILQ_REMOVE(&imgr->pending_read_head,cio,link);
        }
        else{
            TAILQ_INIT(&cio->cio_head);
            hashmap_put(cmap,&cio->key,sizeof(cio->key),cio);
        }
    }
    hashmap_free(cmap);

    //Remove pages interleaving for speeding up loading
    struct pio_head phead;
    TAILQ_INIT(&phead);
    map_t pmap = hashmap_new();

    TAILQ_FOREACH_SAFE(cio,&imgr->pending_read_head,link,ctmp){
        TAILQ_REMOVE(&imgr->pending_read_head,cio,link);
        if(cio->nb_pages==1){
            _load_pages_one_page(pmap,cio,imgr,&phead);
        }
        else{
            _load_pages_multipages(pmap,cio,imgr,&phead);
        }
    }

    struct page_io *pio, *ptmp = NULL;
    TAILQ_FOREACH_SAFE(pio,&phead,link,ptmp){
        TAILQ_REMOVE(&phead,pio,link);
        events++;
        imgr->nb_pending_io++;
        spdk_blob_io_read(pio->blob,pio->imgr->channel,pio->buf,pio->start_page,pio->len,
                           _default_page_io_complete_cb,pio);
    }

    hashmap_free(pmap);
    return events;
}
