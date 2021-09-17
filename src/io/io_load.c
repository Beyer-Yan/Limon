#include <assert.h>
#include "io.h"
#include "slab.h"
#include "kverrno.h"
#include "hashmap.h"

#include "spdk/thread.h"

static inline void _read_pages(struct spdk_blob *blob, uint64_t io_unit_size, struct spdk_io_channel *channel,
		       void *payload, uint64_t offset, uint64_t length,
		       spdk_blob_op_complete cb_fn, void *cb_arg){

    uint64_t io_unit_per_page = KVS_PAGE_SIZE/io_unit_size;
    uint64_t io_unit_offset = offset*io_unit_per_page;
    uint64_t io_uint_length = length*io_unit_per_page;

    spdk_blob_io_read(blob,channel,payload,io_unit_offset,io_uint_length,cb_fn,cb_arg);
}

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

        uint64_t del_tmp;
        hashmap_remove(cio->imgr->read_hash.cache_hash,cio->key,&del_tmp);
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

    uint64_t del_tmp;
    hashmap_remove(pio->imgr->read_hash.page_hash,pio->key,&del_tmp);
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

    uint64_t del_tmp;
    hashmap_remove(pio->imgr->read_hash.page_hash,pio->key,&del_tmp);
}

static void
_default_cache_io_complete_cb(void*ctx, int kverrno){
    struct cache_io *cio = ctx;
    cio->imgr->nb_pending_io--;
    _process_cache_io(cio,kverrno);
}

static void
_load_pages_multipages(struct iomgr* imgr,struct spdk_blob* blob,
                       uint64_t key_prefix,uint8_t* buf,uint64_t start_page,uint64_t nb_pages,
                       struct cache_io *cio){

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

    pio_n = pool_get(imgr->page_io_pool);
    assert(pio_n!=NULL);
    pio_n->cache_io = cio;
    pio_n->key = page_n_key;
    pio_n->imgr = imgr;
    pio_n->io_link = NULL;
    
    hashmap_get(imgr->read_hash.page_hash,page_1_key,&tmp_1);
    hashmap_get(imgr->read_hash.page_hash,page_n_key,&tmp_n);

    //Both ends pages are being loaded.
    if(tmp_1!=NULL && tmp_n!=NULL){
        //All end pages are loading.
        TAILQ_INSERT_TAIL(&tmp_1->pio_head,pio_1,link);
        TAILQ_INSERT_TAIL(&tmp_n->pio_head,pio_n,link);
        if(nb_pages>2){
            //I need load page 2 to n-1
            cio->nb_segments++;
            imgr->nb_pending_io++;
            _read_pages(blob,imgr->io_unit_size,imgr->channel,
                        buf+KVS_PAGE_SIZE,start_page+1,nb_pages-2,
                        _default_cache_io_complete_cb,cio);
        }
    }
    else if(tmp_1==NULL && tmp_n==NULL){
        //Both two pages are not loaded
        TAILQ_INIT(&pio_1->pio_head);
        TAILQ_INIT(&pio_n->pio_head);

        pio_1->io_link = pio_n;

        hashmap_put(imgr->read_hash.page_hash,page_1_key,pio_1);
        hashmap_put(imgr->read_hash.page_hash,page_n_key,pio_n);

        imgr->nb_pending_io++;
        _read_pages(blob,imgr->io_unit_size,imgr->channel,
                    buf,start_page,nb_pages,
                    _default_page_io_complete_cb,pio_1);
    }
    else{
        struct page_io* _pio;
        if(tmp_1!=NULL){
            start_page++;
            buf = buf + KVS_PAGE_SIZE;
            TAILQ_INSERT_TAIL(&tmp_1->pio_head,pio_1,link);
            TAILQ_INIT(&pio_n->pio_head);
            hashmap_put(imgr->read_hash.page_hash,page_n_key,pio_n);
            _pio = pio_n;
        }
        else{
            TAILQ_INSERT_TAIL(&tmp_n->pio_head,pio_n,link);
            TAILQ_INIT(&pio_1->pio_head);
            hashmap_put(imgr->read_hash.page_hash,page_1_key,pio_1); 
            _pio = pio_1;
        }
        imgr->nb_pending_io++;
        _read_pages(blob,imgr->io_unit_size,imgr->channel,
                    buf,start_page,nb_pages-1,
                    _default_page_io_complete_cb,_pio);
    }
}

static void 
_load_pages_one_page(struct iomgr* imgr,struct spdk_blob* blob,
                     uint64_t key_prefix,uint8_t* buf,uint64_t start_page, 
                     struct cache_io *cio){
    struct page_io* pio = NULL;
    struct page_io* tmp = NULL;

    pio = pool_get(imgr->page_io_pool);
    assert(pio!=NULL);

    pio->cache_io = cio;
    pio->key = key_prefix;
    pio->imgr = imgr;
    pio->io_link = NULL;

    hashmap_get(imgr->read_hash.page_hash,key_prefix,&tmp);
    if(tmp!=NULL){
        //Someone else is already loading this page.
        TAILQ_INSERT_TAIL(&tmp->pio_head,pio,link);
    }
    else{
        TAILQ_INIT(&pio->pio_head);
        hashmap_put(imgr->read_hash.page_hash,pio->key,pio);
        //Now issue a blob IO command for pio_1_pages;
        imgr->nb_pending_io++;
        _read_pages(blob,imgr->io_unit_size,imgr->channel,
                    buf,start_page,1,
                    _default_page_io_complete_cb,pio);
    }
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
    cio->key = _make_cache_key(key_prefix,nb_pages);

    hashmap_get(imgr->read_hash.cache_hash,cio->key,&tmp);
    if(tmp!=NULL){
        //Other IOs are already prefetching the same pages!
        TAILQ_INSERT_TAIL(&tmp->cio_head,cio,link);
        return;
    }
    cio->cnt = 0;
    TAILQ_INIT(&cio->cio_head);
    hashmap_put(imgr->read_hash.cache_hash,cio->key,cio);

    if(nb_pages==1){
        cio->nb_segments = 1;
        _load_pages_one_page(imgr,blob,key_prefix,buf,start_page,cio);
    }
    else{
        //Two-phase loading.
        cio->nb_segments = 2;
        _load_pages_multipages(imgr,blob,key_prefix,buf,start_page,nb_pages,cio);
    }
}
