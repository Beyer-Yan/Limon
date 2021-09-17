#include <stdbool.h>
#include <time.h>
#include <assert.h>
#include <stdlib.h>
#include "kverrno.h"
#include "pagechunk.h"
#include "slab.h"
#include "rbtree_uint.h"
#include "item.h"
#include "pool.h"
#include "spdk/queue.h"

#include "spdk/log.h"

#include "../worker/worker_internal.h"

static uint64_t 
_calc_tsc(void){
    //44 bits second, 20 bits microsecond
    struct tsc {
        uint64_t usec:20; //20 low bits
        uint64_t sec:44;  //44 high bits
    }t;

    struct timespec time;
    clock_gettime(CLOCK_REALTIME,&time);
    t.sec = time.tv_sec;
    t.usec = (time.tv_nsec/1000);
    
    return *(uint64_t*)&t;
}

static void _bump_page(struct pagechunk_mgr *pmgr,struct page_desc* pdesc){
    assert(pmgr);
    assert(pdesc);
    TAILQ_REMOVE(&pmgr->pages_head,pdesc,link);
    TAILQ_INSERT_TAIL(&pmgr->pages_head,pdesc,link);
}

static uint64_t
_get_page_position(struct chunk_desc *desc, uint64_t slot_idx, 
              uint32_t *first_page_out, uint32_t *last_page_out){
    uint32_t slab_size = desc->slab->slab_size;
    uint32_t offset = slot_idx%desc->slab->reclaim.nb_slots_per_chunk;

    if(slab_size>=MULTI_PAGE_SLAB_SIZE){
        //Overflow will not happen, just do it
        *first_page_out = offset * slab_size / KVS_PAGE_SIZE;
        *last_page_out = ((offset+1) * slab_size - 1) / KVS_PAGE_SIZE;

        //int start_offset = (offset * slab_size % KVS_PAGE_SIZE);
        return offset * slab_size;
    }
    else{
        //The item is not allowed to store across pages.
        uint32_t slots_per_page = KVS_PAGE_SIZE/slab_size;
        *first_page_out = offset/slots_per_page;
        *last_page_out = *first_page_out;

        uint32_t start_offset = offset%slots_per_page;
        return (*first_page_out)*KVS_PAGE_SIZE + start_offset*slab_size;
    }
}

static bool
_is_shared_page(struct chunk_desc *desc, uint64_t slot_idx, bool first){
    uint32_t slab_size = desc->slab->slab_size;
    uint32_t offset = slot_idx%desc->slab->reclaim.nb_slots_per_chunk;
    if(slab_size<MULTI_PAGE_SLAB_SIZE){
        //The slot must be in a shared page.
        return true;
    }
    else{
        bool share_first  = (offset * slab_size % KVS_PAGE_SIZE) != 0;
        bool share_last   = (((offset+1) * slab_size) % KVS_PAGE_SIZE) != 0;

        if(slab_size<=KVS_PAGE_SIZE){
            return share_first|share_last;
        }
        else{
            return first ? share_first : share_last;
        }
    }
}

void pagechunk_mem_lift(struct pagechunk_mgr *pmgr,struct chunk_desc* desc){
    assert(pmgr);
    assert(desc);

    if(!desc->nb_pendings){
        desc->dma_buffer = dma_buffer_pool_pop(pmgr->dma_pool);
        
    }

    assert(desc->dma_buffer);
    desc->nb_pendings++;
}

void pagechunk_mem_lower(struct pagechunk_mgr *pmgr,struct chunk_desc* desc){
    assert(pmgr);
    assert(desc);
    assert(desc->dma_buffer);
    assert(desc->nb_pendings>0);
    desc->nb_pendings--;

    if(!desc->nb_pendings){
        //release the dma buffer
        dma_buffer_pool_push(pmgr->dma_pool,desc->dma_buffer);
        desc->dma_buffer = NULL;
    }
}

bool 
pagechunk_is_cross_page(struct chunk_desc *desc, uint64_t slot_idx){
    
    if(desc->slab->slab_size>KVS_PAGE_SIZE){
        return true;
    }
    if(desc->slab->slab_size<MULTI_PAGE_SLAB_SIZE){
        return false;
    }

    uint32_t first_page, last_page;
    _get_page_position(desc,slot_idx,&first_page,&last_page);

    return (first_page == last_page) ? false : true;
}

struct kv_item*
pagechunk_get_item(struct pagechunk_mgr *chunk_mgr,struct chunk_desc *desc, uint64_t slot_idx){
    assert(desc->dma_buffer!=NULL);

    uint32_t first_page, last_page;
    uint64_t addr_offset = _get_page_position(desc,slot_idx,&first_page,&last_page);
    
    //Remove 8 bytes tsc;
    return (struct kv_item*)(desc->dma_buffer + addr_offset + 8);
}

void 
pagechunk_put_item(struct pagechunk_mgr *chunk_mgr,struct chunk_desc *desc, uint64_t slot_idx,struct kv_item* item){
    assert(desc->dma_buffer!=NULL);
    assert(slab_is_valid_size(desc->slab->slab_size,item_packed_size(item)));

    uint32_t first_page, last_page;
    uint64_t addr_offset;
    uint64_t tsc;
    
    addr_offset = _get_page_position(desc,slot_idx,&first_page,&last_page);
    tsc = _calc_tsc();

    uint8_t* slot_addr = desc->dma_buffer + addr_offset;

    //Fill the 8 bytes timestamp in the header.
    memcpy(slot_addr,&tsc,8);
    slot_addr += 8;

    //Fill the item
    uint32_t item_size = item_get_size(item);
    memcpy(slot_addr,item,item_size);
    slot_addr += item_size;

    //Fill the 8 bytes timestamp in the tail.
    memcpy(slot_addr,&tsc,8);
}

static void
_page_load_complete_cb_fn(void* ctx, int kverrno){
    struct page_load_store_ctx* page_ctx  = ctx;
    struct pagechunk_mgr *pmgr = page_ctx->pmgr;
    struct chunk_desc *desc = page_ctx->desc;
    uint32_t page_offset  = page_ctx->page_offset;

    if(kverrno){
        //error happens
        SPDK_ERRLOG("Error in loading,slab:%u,desc:%u,page:%u,err:%d\n",
                    desc->slab->slab_size,
                    desc->id,
                    page_offset,
                    kverrno);
        assert(0 && "just crash");
    }
    else{
        //Now, the new page has been successfully loaded
        uint64_t page_key = (uint64_t)desc + page_offset;

        //check whether the page has been put into page hash index
        struct page_desc* pdesc = NULL;
        hashmap_get(pmgr->page_map,page_key,&pdesc);
        if(!pdesc){
            //allocate a page descriptor, then put it into page hash index
            pdesc = pagechunk_evict_one_page(pmgr);
            pdesc->key = page_key;
            hashmap_put(pmgr->page_map,page_key,pdesc);

            //bump the LRU cache
            TAILQ_INSERT_TAIL(&pmgr->pages_head,pdesc,link);

            //copy the data into page cache;
            memcpy(pdesc->data,desc->dma_buffer+page_offset*KVS_PAGE_SIZE, KVS_PAGE_SIZE);
        }
    }

    struct item_load_store_ctx* item_ctx = page_ctx->item_ctx;
    pool_release(pmgr->load_store_ctx_pool,page_ctx);

    item_ctx->kverrno = kverrno ? kverrno : -KV_ESUCCESS;
    item_ctx->cnt++;
    if(item_ctx->cnt == item_ctx->nb_segs){
        //Now all segments have been loaded
        item_ctx->user_cb(item_ctx->user_ctx,kverrno);
        pool_release(pmgr->item_ctx_pool,item_ctx);
    }
}

void 
pagechunk_load_item_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx){
    assert(desc->dma_buffer);

    uint32_t first_page, last_page;
    struct slab *slab = desc->slab;
    _get_page_position(desc,slot_idx,&first_page,&last_page);
    uint32_t nb_pages = last_page - first_page + 1;

    struct page_desc* pdesc[nb_pages];
    uint32_t nb_segs = 0;
    bool toggle = true;

    struct scatter_loader{
        uint16_t start_page;
        uint16_t nb_pages;
    }scatter_array[nb_pages];
    memset(scatter_array,0,sizeof(scatter_array[0])*nb_pages);

    assert(nb_pages<=UINT16_MAX);

    for(uint32_t i=0;i<=nb_pages;i++){
        uint32_t page_off = first_page+i;
        uint64_t page_key = (uint64_t)desc + page_off;
        hashmap_get(pmgr->page_map,page_key,&pdesc[i]);   
        if(pdesc[i]){
            toggle = true;
            //Just copy the data buffer from the cache.
            memcpy(desc->dma_buffer+page_off*KVS_PAGE_SIZE,pdesc[i]->data,KVS_PAGE_SIZE);
            _bump_page(pmgr,pdesc[i]);
        }

        if(!pdesc[i] && toggle){
            scatter_array[nb_segs].start_page = i;
            scatter_array[nb_segs].nb_pages = 1;
            nb_segs++;

            toggle = false;
            pmgr->miss_times++;
        }
        else if(!pdesc[i]){
            scatter_array[nb_segs-1].nb_pages++;
            pmgr->miss_times++;
        }
        pmgr->visit_times++;
    }

    //All pages have been cached
    if(!nb_segs){
        //All pages are cached, do nothing
        cb(ctx,-KV_ESUCCESS);
        return;
    }

    //Now i should load the uncached pages
    struct item_load_store_ctx* item_ctx = pool_get(pmgr->item_ctx_pool);
    item_ctx->kverrno = -KV_ESUCCESS;
    item_ctx->nb_segs = nb_segs;
    item_ctx->cnt = 0;
    item_ctx->user_cb = cb;
    item_ctx->user_ctx = ctx;

    for(uint32_t i=0;i<nb_segs;i++){
        //load it from disk
        struct page_load_store_ctx* page_ctx = pool_get(pmgr->load_store_ctx_pool);
        assert(page_ctx!=NULL);
        uint32_t page_offset = scatter_array[i].start_page;
        uint32_t nb_pages = scatter_array[i].nb_pages;

        page_ctx->pmgr = pmgr;
        page_ctx->desc = desc;
        page_ctx->page_offset = page_offset;
        page_ctx->item_ctx = item_ctx;

        uint32_t nb_chunk_pages = desc->slab->reclaim.nb_pages_per_chunk;
        uint64_t start_page_in_slab = nb_chunk_pages * desc->id + page_offset;
        uint8_t* buf = desc->dma_buffer + page_offset*KVS_PAGE_SIZE;
        uint64_t key_prefix = (uint64_t)buf;

        iomgr_load_pages_async(imgr,slab->blob,key_prefix,
                    buf,start_page_in_slab,nb_pages,
                    _page_load_complete_cb_fn,page_ctx);
    }
}

void 
pagechunk_load_item_share_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx){
    assert(desc->dma_buffer);

    uint32_t first_page, last_page;
    struct slab *slab = desc->slab;
    _get_page_position(desc,slot_idx,&first_page,&last_page);

    //No more than 2 shared pages for an slot.
    uint32_t nb_segs = 2;
    struct page_desc* pdesc[2];

    //If the first page is not shared page, I need not load it.
    //If the fitst page is a shared page, then I should check the cached state.
    uint64_t page0_key = (uint64_t)desc + first_page;
    uint64_t page1_key = (uint64_t)desc + last_page;
    hashmap_get(pmgr->page_map,page0_key,&pdesc[0]);
    hashmap_get(pmgr->page_map,page1_key,&pdesc[1]);

    if( !_is_shared_page(desc,slot_idx,true)){
        first_page=UINT32_MAX;
        nb_segs--;
    }
    if( !_is_shared_page(desc,slot_idx,false)) {
        last_page=UINT32_MAX;
        nb_segs--;
    }

    if(!nb_segs){
        //Wonderful! All shared pages has been cached.
        cb(ctx,-KV_ESUCCESS);
        return;
    }

    //Load the uncached pages
    struct item_load_store_ctx* item_ctx = pool_get(pmgr->item_ctx_pool);
    item_ctx->kverrno = -KV_ESUCCESS;
    item_ctx->nb_segs = nb_segs;
    item_ctx->cnt = 0;
    item_ctx->user_cb = cb;
    item_ctx->user_ctx = ctx;

    for(int i=0;i<2;i++){
        uint32_t page = i==0? first_page : last_page;
        struct page_desc* _pdesc = pdesc[i];

        if(page!=UINT32_MAX){
            if(_pdesc){
                //just load it from the cache
                memcpy(desc->dma_buffer+page*KVS_PAGE_SIZE,_pdesc->data,KVS_PAGE_SIZE);
                _bump_page(pmgr,_pdesc);
            }
        }
        else{
            struct page_load_store_ctx* page_ctx = pool_get(pmgr->load_store_ctx_pool);
            assert(page_ctx!=NULL);

            page_ctx->pmgr = pmgr;
            page_ctx->desc = desc;
            page_ctx->page_offset = page;
            page_ctx->item_ctx = item_ctx;

            uint32_t nb_chunk_pages = desc->slab->reclaim.nb_pages_per_chunk;
            uint64_t start_page_in_slab = nb_chunk_pages * desc->id + page;
            uint8_t* buf = desc->dma_buffer + page*KVS_PAGE_SIZE;
            uint64_t key_prefix = (uint64_t)buf;

            iomgr_load_pages_async(imgr,slab->blob,key_prefix,
                                   buf,start_page_in_slab,1,
                                   _page_load_complete_cb_fn,page_ctx);
            pmgr->miss_times++;
        }
        pmgr->visit_times++;
    }
}

void pagechunk_load_item_meta_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx){
    assert(desc->dma_buffer);

    uint32_t first_page, last_page;
    struct slab *slab = desc->slab;
    _get_page_position(desc,slot_idx,&first_page,&last_page);

    //If the first page is not shared page, I need not load it.
    //If the fitst page is a shared page, then I should check the cached state.
    struct page_desc* pdesc = NULL;
    uint64_t page_key = (uint64_t)desc + first_page;
    hashmap_get(pmgr->page_map,&page_key,&pdesc);

    if(pdesc){
        //copy from page cache
        memcpy(desc->dma_buffer+first_page*KVS_PAGE_SIZE,pdesc->data,KVS_PAGE_SIZE);
        _bump_page(pmgr,pdesc);
        cb(ctx,-KV_ESUCCESS);
        pmgr->visit_times++;
        return;
    }

    //Load the uncached pages
    struct item_load_store_ctx* item_ctx = pool_get(pmgr->item_ctx_pool);
    item_ctx->kverrno = -KV_ESUCCESS;
    item_ctx->nb_segs = 1;
    item_ctx->cnt = 0;
    item_ctx->user_cb = cb;
    item_ctx->user_ctx = ctx;

    struct page_load_store_ctx* page_ctx = pool_get(pmgr->load_store_ctx_pool);
    assert(page_ctx!=NULL);

    page_ctx->pmgr = pmgr;
    page_ctx->desc = desc;
    page_ctx->page_offset = first_page;
    page_ctx->item_ctx = item_ctx;

    uint32_t nb_chunk_pages = desc->slab->reclaim.nb_pages_per_chunk;
    uint64_t start_page_in_slab = nb_chunk_pages * desc->id + first_page;
    uint8_t* buf = desc->dma_buffer + first_page*KVS_PAGE_SIZE;
    uint64_t key_prefix = (uint64_t)buf;

    iomgr_load_pages_async(imgr,slab->blob,key_prefix,
                            buf,start_page_in_slab,1,
                            _page_load_complete_cb_fn,page_ctx);
    pmgr->miss_times++;
    pmgr->visit_times++;
}

static void _copy_data_to_cache(struct pagechunk_mgr *pmgr,
                         struct page_desc* pdesc,uint8_t* buffer_base,
                         uint64_t offset,uint32_t size){
    if(!pdesc){
        //should copy the whole page
        pdesc = pagechunk_evict_one_page(pmgr);
        uint32_t page_off = (offset/KVS_PAGE_SIZE);
        uint32_t page_base = page_off*KVS_PAGE_SIZE;
        pdesc->key = (uint64_t)pdesc + page_off;

        hashmap_put(pmgr->page_map,pdesc->key,pdesc);
        memcpy(pdesc->data,buffer_base+page_base,KVS_PAGE_SIZE);

        //Bump the LRU
        TAILQ_INSERT_TAIL(&pmgr->pages_head,pdesc,link);
        pmgr->miss_times++;
    }else{
        //Just copy the delta data
        memcpy(pdesc->data,buffer_base+offset,size);
    }
    _bump_page(pmgr,pdesc);
    pmgr->visit_times++;
}

static void _fill_page_cache( struct pagechunk_mgr *pmgr,
                       struct page_desc** pdesc, int nb_pages, 
                       uint8_t* buffer_base, uint64_t offset,uint32_t size){
    assert(pmgr);
    assert(pdesc);

    uint32_t page = offset/KVS_PAGE_SIZE;
    uint32_t gap = KVS_PAGE_SIZE - (offset%KVS_PAGE_SIZE);
    if(size <= gap){
        _copy_data_to_cache(pmgr,pdesc[page],buffer_base,offset,size);
    }else{
        uint32_t nb_middle_pages = (size-gap)/KVS_PAGE_SIZE;
        uint32_t last_size = (size-gap)%KVS_PAGE_SIZE;

        //first page
        _copy_data_to_cache(pmgr,pdesc[page],buffer_base,offset,gap);
        offset += gap;

        //middle pages
        for(uint32_t i=0;i<nb_middle_pages;i++){
            uint32_t page = offset/KVS_PAGE_SIZE;
            _copy_data_to_cache(pmgr,pdesc[page],buffer_base,offset,KVS_PAGE_SIZE);
            offset += KVS_PAGE_SIZE;
        }

        //last page
        if(nb_middle_pages && last_size>0){
            uint32_t page = offset/KVS_PAGE_SIZE;
            _copy_data_to_cache(pmgr,pdesc[page],buffer_base,offset,last_size);
        }
    }
}

static void
_item_store_complete_cb_fn(void* ctx, int kverrno){
    struct page_load_store_ctx* page_ctx  = ctx;
    struct pagechunk_mgr *pmgr = page_ctx->pmgr;
    struct chunk_desc *desc = page_ctx->desc;
    uint32_t page_offset  = page_ctx->page_offset;

    if(kverrno){
        //error happens
        SPDK_ERRLOG("Error in loading,slab:%u,desc:%u,page:%u,err:%d\n",
                    desc->slab->slab_size,
                    desc->id,
                    page_offset,
                    kverrno);
        assert(0 && "just crash");
    }

    struct item_load_store_ctx* item_ctx = page_ctx->item_ctx;
    uint32_t nb_pages = page_ctx->nb_pages;
    uint8_t* buffer_addr = desc->dma_buffer;
    uint64_t offset = item_ctx->slot_addr_offset;
    struct page_desc* pdesc[nb_pages];

    for(uint32_t i=0;i<nb_pages;i++){
        uint64_t page_key = (uint64_t)desc + page_offset + i;
        hashmap_get(pmgr->page_map,page_key,&pdesc[i]);
    }
    _fill_page_cache(pmgr,pdesc,nb_pages,buffer_addr,offset,item_ctx->size);

    item_ctx->user_cb(item_ctx->user_ctx,kverrno);
    pool_release(pmgr->load_store_ctx_pool,page_ctx);
    pool_release(pmgr->item_ctx_pool,item_ctx);
}

void 
pagechunk_store_item_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx){
    assert(desc->dma_buffer);
    uint32_t first_page, last_page;
    struct slab *slab = desc->slab;

    uint64_t slot_addr = _get_page_position(desc,slot_idx,&first_page,&last_page);

    struct item_load_store_ctx* item_ctx = pool_get(pmgr->item_ctx_pool);
    assert(item_ctx!=NULL);

    item_ctx->kverrno = -KV_ESUCCESS;
    item_ctx->cnt = 0;
    item_ctx->nb_segs = 1;
    item_ctx->slot_addr_offset = slot_addr;
    item_ctx->size = slab->slab_size;
    item_ctx->user_cb = cb;
    item_ctx->user_ctx = ctx;

    struct page_load_store_ctx* page_ctx = pool_get(pmgr->load_store_ctx_pool);
    assert(page_ctx);

    page_ctx->pmgr = pmgr;
    page_ctx->desc = desc;
    page_ctx->item_ctx = item_ctx;
    page_ctx->page_offset = first_page;
    page_ctx->nb_pages = last_page - first_page + 1;

    uint32_t nb_chunk_pages = desc->slab->reclaim.nb_pages_per_chunk;
    uint64_t start_page_in_slab = nb_chunk_pages * desc->id + first_page;
    uint64_t nb_pages = last_page - first_page + 1;
    uint8_t* buf = desc->dma_buffer + first_page*KVS_PAGE_SIZE;
    uint64_t key_prefix = (uint64_t)buf;

    iomgr_store_pages_async(imgr,slab->blob,key_prefix,
                            buf,start_page_in_slab,nb_pages,
                            _item_store_complete_cb_fn,page_ctx);
}

void pagechunk_store_item_meta_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx){
    assert(desc->dma_buffer);
    uint32_t first_page, last_page;
    struct slab *slab = desc->slab;

    uint64_t slot_addr = _get_page_position(desc,slot_idx,&first_page,&last_page);

    struct item_load_store_ctx* item_ctx = pool_get(pmgr->item_ctx_pool);
    assert(item_ctx!=NULL);

    item_ctx->kverrno = -KV_ESUCCESS;
    item_ctx->cnt = 0;
    item_ctx->nb_segs = 1;
    item_ctx->slot_addr_offset = slot_addr;
    item_ctx->size = sizeof(struct item_meta)+8;
    item_ctx->user_cb = cb;
    item_ctx->user_ctx = ctx;

    struct page_load_store_ctx* page_ctx = pool_get(pmgr->load_store_ctx_pool);
    assert(page_ctx);

    page_ctx->pmgr = pmgr;
    page_ctx->desc = desc;
    page_ctx->item_ctx = item_ctx;
    page_ctx->page_offset = first_page;
    page_ctx->nb_pages = 1;

    uint32_t nb_chunk_pages = desc->slab->reclaim.nb_pages_per_chunk;
    uint64_t start_page_in_slab = nb_chunk_pages * desc->id + first_page;
    uint64_t nb_pages = 1;
    uint8_t* buf = desc->dma_buffer + first_page*KVS_PAGE_SIZE;
    uint64_t key_prefix = (uint64_t)buf;

    iomgr_store_pages_async(imgr,slab->blob,key_prefix,
                            buf,start_page_in_slab,nb_pages,
                            _item_store_complete_cb_fn,page_ctx);
}

struct page_desc* pagechunk_evict_one_page(struct pagechunk_mgr *pmgr){
    struct page_desc *pdesc = NULL;

    if(pmgr->nb_used_pages<pmgr->nb_init_pages){
        //Just allocate a new page.
        pdesc = &pmgr->pdesc_arr[pmgr->nb_used_pages++];
    }
    else{
        pdesc = TAILQ_FIRST(&pmgr->pages_head);
        assert(pdesc);

        TAILQ_REMOVE(&pmgr->pages_head,pdesc,link);

        uint64_t page_key = pdesc->key;
        struct page_desc *tmp;
        hashmap_remove(pmgr->page_map,page_key,&tmp);
        assert(tmp || tmp==pdesc);
    }

    pdesc->key = 0;
    return pdesc;   
}

// struct chunk_mem*
// pagechunk_evict_one_chunk(struct pagechunk_mgr *pmgr){
//     struct chunk_mem * mem = NULL;
//     struct chunk_desc *desc = NULL;

//     TAILQ_FOREACH(desc,&pmgr->global_chunks,link){
//         if(!(desc->flag&CHUNK_PIN)){
//             mem = desc->chunk_mem;
//             desc->chunk_mem=NULL;
//             break;
//         }
//     }
//     if(mem){
//         //Remove it from the lru list.
//         TAILQ_REMOVE(&pmgr->global_chunks,desc,link);
//         pmgr->nb_used_chunks--;
//     }
//     else{
//         //I should perform chunk evicting, but now I have no chunks to evict.
//         //The nbumber of chunks are too small.
//         //This is a system bug.
//         SPDK_ERRLOG("Error in chunk request:The number of chunks are too small.");
//         assert(0);
//     }
//     return mem;
// }

// //This callback shall be exceuted in the original polling thread.
// static void _remote_chunk_mem_request_finish(void*ctx){
//     struct chunk_miss_callback *cb_obj = ctx;
//     struct pagechunk_mgr *pmgr = cb_obj->requestor_pmgr;
//     struct chunk_desc *desc = cb_obj->desc;

//     int kverrno = cb_obj->kverrno;
//     struct chunk_mem* mem = cb_obj->mem;

//     if(!kverrno){
//         //SPDK_NOTICELOG("Success in getting one chunk, err:%d\n",kverrno);
//         bitmap_clear_bit_all(mem->bitmap);
//         desc->chunk_mem = mem;
//         TAILQ_INSERT_TAIL(&pmgr->global_chunks,desc,link);
//         pmgr->nb_used_chunks++;
//     }

//     //SPDK_NOTICELOG("Get one chunk mem:%p\n",mem);

//     struct chunk_miss_callback *tmp=NULL;

//     TAILQ_FOREACH_SAFE(cb_obj,&desc->chunk_miss_callback_head,link,tmp){
//         TAILQ_REMOVE(&desc->chunk_miss_callback_head,cb_obj,link);
//         pool_release(pmgr->kv_chunk_request_pool,cb_obj);
//         cb_obj->cb_fn(cb_obj->ctx,kverrno);
//     }
// }

// static bool
// _pagechunk_local_evaluate(struct pagechunk_mgr *pmgr){
//     //experience value.
//     uint64_t threshold = pmgr->water_mark*9/10;
//     if(pmgr->nb_used_chunks<=threshold){
//         return false;
//     }
//     else{
//         static int beta  = 0;
//         int miss_rate = pmgr->miss_times*100/pmgr->visit_times;
//         int util_rate = pmgr->nb_used_chunks*100/(4*pmgr->water_mark);
//         int p = beta*miss_rate*(100-util_rate)/100;

//         p = p<100 ? p : 100;
//         int god_decision = rand_r(&pmgr->seed)%100;

//         //SPDK_NOTICELOG("Local evaluating, p:%d, decision:%d\n",p,god_decision);
//         //Now listen to the God.
//         return (god_decision<p) ? false : true ;
//     }
// }

// void pagechunk_request_one_async(struct pagechunk_mgr *pmgr,
//                                  struct chunk_desc* desc,
//                                  void(*cb)(void*ctx,int kverrno), 
//                                  void* ctx){
//     #if 0
//     struct chunk_mem* mem = NULL;
//     if(_pagechunk_local_evaluate(pmgr)){
//         mem = pagechunk_evict_one_chunk(pmgr);
//     }else{
//         mem = chunkmgr_request_one(pmgr);
//         if(!mem){
//             mem = pagechunk_evict_one_chunk(pmgr);
//         }else{
//             pmgr->nb_used_chunks++;
//         }
//     }

//     bitmap_clear_bit_all(mem->bitmap);
//     desc->chunk_mem = mem;
//     TAILQ_INSERT_TAIL(&pmgr->global_chunks,desc,link);
//     cb(ctx,-KV_ESUCCESS);
//     #endif
    
//     //In such case, I should send a request to the global chunk memory manager.
//     //The global chunk maneger will deside how I should get a chunk memory, 
//     //ether from the local evicting, or from the remote evicting , or allocating
//     //from the global chunk manager. It's in God's hands !!
//     struct chunk_miss_callback *cb_obj = pool_get(pmgr->kv_chunk_request_pool);
//     assert(cb_obj!=NULL);
//     cb_obj->requestor_pmgr = pmgr;
//     cb_obj->desc  = desc;
//     cb_obj->cb_fn = cb;
//     cb_obj->ctx   = ctx;

//     if(!TAILQ_EMPTY(&desc->chunk_miss_callback_head)){
//         TAILQ_INSERT_TAIL(&desc->chunk_miss_callback_head,cb_obj,link);
//     }   
//     else if(_pagechunk_local_evaluate(pmgr)){
//             //I should perform local evicting instead of requesting chunk memory from
//             //global chunk manager, since the cost of cross-core communication is much
//             //higher than local evicting.
//             cb_obj->mem = pagechunk_evict_one_chunk(pmgr);
//             cb_obj->kverrno = -KV_ESUCCESS;
//             TAILQ_INSERT_TAIL(&desc->chunk_miss_callback_head,cb_obj,link);
//             _remote_chunk_mem_request_finish(cb_obj);
//     }
//     else{
//         //SPDK_NOTICELOG("New chunk mem request\n");
//         cb_obj->finish_cb = _remote_chunk_mem_request_finish;
//         TAILQ_INSERT_TAIL(&desc->chunk_miss_callback_head,cb_obj,link);
//         chunkmgr_request_one_aysnc(cb_obj);
//     }
// }

// void pagechunk_release_one(struct pagechunk_mgr *pmgr,
//                             struct chunk_mem* mem){
//     chunkmgr_release_one(pmgr,mem);
// }
