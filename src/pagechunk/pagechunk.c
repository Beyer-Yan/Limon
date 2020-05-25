#include <stdbool.h>
#include <time.h>
#include <assert.h>
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
    uint64_t tsc = 0;;
    struct timespec time;
    clock_gettime(CLOCK_MONOTONIC,&time);
    tsc = (uint64_t)time.tv_sec<<20;
    tsc = tsc | ((time.tv_nsec/1000)>>44);
    return tsc;
}

static uint8_t*
_get_position(struct chunk_desc *desc, uint64_t slot_idx, 
              uint32_t *first_page_out, uint32_t *last_page_out){
    uint32_t slab_size = desc->slab_size;
    uint32_t offset = slot_idx%desc->nb_slots;

    if(slab_size>=MULTI_PAGE_SLAB_SIZE){
        //Overflow will not happen, just do it
        *first_page_out = offset * slab_size / KVS_PAGE_SIZE;
        *last_page_out = ((offset+1) * slab_size - 1) / KVS_PAGE_SIZE;

        //int start_offset = (offset * slab_size % KVS_PAGE_SIZE);
        return desc->chunk_mem->data + offset * slab_size;
    }
    else{
        //The item is not allowed to store across pages.
        uint32_t slots_per_page = KVS_PAGE_SIZE/slab_size;
        *first_page_out = offset/slots_per_page;
        *last_page_out = *first_page_out;

        uint32_t start_offset = offset%slots_per_page;
        return desc->chunk_mem->data + (*first_page_out)*KVS_PAGE_SIZE + start_offset*slab_size;
    }
}

bool 
pagechunk_is_cached(struct chunk_desc *desc, uint64_t slot_idx){
    
    uint32_t first_page, last_page;
    _get_position(desc,slot_idx,&first_page,&last_page);

    if(first_page == last_page){
        //The item is stored in single page.
        return (bitmap_get_bit(desc->chunk_mem->bitmap,first_page) ? true : false);
    }
    else{
        //The item is stored more than one pages.
        if(!bitmap_get_bit(desc->chunk_mem->bitmap,first_page)){
            return false;
        }
        return (bitmap_get_bit(desc->chunk_mem->bitmap,first_page+1) ? true : false);
    }
}

bool 
pagechunk_is_cross_page(struct chunk_desc *desc, uint64_t slot_idx){
    
    if(desc->slab_size>KVS_PAGE_SIZE){
        return true;
    }
    if(desc->slab_size<MULTI_PAGE_SLAB_SIZE){
        return false;
    }

    uint32_t first_page, last_page;
    _get_position(desc,slot_idx,&first_page,&last_page);

    return (first_page == last_page) ? false : true;
}

struct kv_item*
pagechunk_get_item(struct pagechunk_mgr *chunk_mgr,struct chunk_desc *desc, uint64_t slot_idx){
   
    assert(desc->chunk_mem!=NULL);

    uint32_t first_page, last_page;
    uint8_t* addr = _get_position(desc,slot_idx,&first_page,&last_page);

    //Bump the LRU list.
    chunk_mgr->hit_times++;
    TAILQ_REMOVE(&chunk_mgr->global_chunks,desc,link);
    TAILQ_INSERT_HEAD(&chunk_mgr->global_chunks,desc,link);
    
    //Remove 8 bytes tsc;
    return (struct kv_item*)(addr + 8);
}

void 
pagechunk_put_item(struct pagechunk_mgr *chunk_mgr,struct chunk_desc *desc, uint64_t slot_idx,struct kv_item* item){
    assert(desc->chunk_mem!=NULL);

    uint32_t first_page, last_page;
    uint8_t *addr;
    uint64_t tsc;
    
    addr = _get_position(desc,slot_idx,&first_page,&last_page);
    tsc = _calc_tsc();

    //Fill the 8 bytes timestamp in the header.
    memcpy(addr,&tsc,8);
    addr += 8;

    //Fill the item
    uint32_t item_size = item_get_size(item);
    memcpy(addr,item,item_size);
    addr += item_size;

    //Fill the 8 bytes timestamp in the tail.
    memcpy(addr,&tsc,8);

    //Bump the LRU list.
    chunk_mgr->hit_times++;
    TAILQ_REMOVE(&chunk_mgr->global_chunks,desc,link);
    TAILQ_INSERT_HEAD(&chunk_mgr->global_chunks,desc,link);
}

static void
_item_load_complete_cb_fn(void* ctx, int kverrno){
    struct chunk_load_store_ctx* cls_ctx  = ctx;
    uint32_t first_page  = cls_ctx->first_page;
    uint32_t last_page = cls_ctx->last_page;

    struct chunk_desc *desc = cls_ctx->desc;

    if(!kverrno){
        //Load success. Set the cache bits.
        bitmap_set_bit_range(desc->chunk_mem->bitmap,first_page,last_page);
    }
    pool_release(cls_ctx->pmgr->load_store_ctx_pool,cls_ctx);
    cls_ctx->user_cb(cls_ctx->user_ctx,kverrno);
}

void 
pagechunk_load_item_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx){
    uint32_t first_page, last_page;
    struct slab *slab = desc->slab;

    _get_position(desc,slot_idx,&first_page,&last_page);

    if(bitmap_get_bit(desc->chunk_mem->bitmap,first_page)){
        first_page++;
    }
    if(bitmap_get_bit(desc->chunk_mem->bitmap,last_page)){
        last_page--;
    }

    if(first_page>last_page){
        //The item has already been cached, so this function shall not be called.
        //If this happens, the program has bug here.
        //assert(0); ???
        cb(ctx,-KV_ESUCCESS);
        return;
    }
    
    uint64_t nb_pages = last_page - first_page + 1;
    uint64_t start_page_in_slab = desc->nb_pages * desc->id + first_page;
    uint8_t *buf = &desc->chunk_mem->data[first_page*KVS_PAGE_SIZE];
    uint64_t key_prefix = (uint64_t)desc + first_page;

    struct chunk_load_store_ctx* cls_ctx = pool_get(pmgr->load_store_ctx_pool);
    assert(cls_ctx!=NULL);
    
    cls_ctx->pmgr = pmgr;
    cls_ctx->desc = desc;
    cls_ctx->slot_idx = slot_idx;
    cls_ctx->first_page = first_page;
    cls_ctx->last_page = last_page;
    cls_ctx->user_cb = cb;
    cls_ctx->user_ctx = ctx;

    iomgr_load_pages_async(imgr,slab->blob,key_prefix,
                           buf,start_page_in_slab,nb_pages,
                           _item_load_complete_cb_fn,cls_ctx);
}

static void
_item_share_load_complete_cb_fn(void* ctx, int kverrno){
    struct chunk_load_store_ctx* cls_ctx  = ctx;
    struct pagechunk_mgr *pmgr = cls_ctx->pmgr;
    struct chunk_desc *desc = cls_ctx->desc;

    uint32_t first_page  = cls_ctx->first_page;
    uint32_t last_page = cls_ctx->last_page;

    cls_ctx->kverrno = kverrno ? kverrno : -KV_ESUCCESS;

    if(++cls_ctx->cnt==cls_ctx->nb_segs){
        if(!cls_ctx->kverrno){
            first_page!=UINT32_MAX ?  bitmap_set_bit(desc->chunk_mem->bitmap,first_page): (void)0;
            last_page !=UINT32_MAX ?  bitmap_set_bit(desc->chunk_mem->bitmap,last_page) : (void)0;
        }
        pool_release(pmgr->load_store_ctx_pool,cls_ctx);
        cls_ctx->user_cb(cls_ctx->user_ctx,cls_ctx->kverrno);
    }
}

void 
pagechunk_load_item_share_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx){
    uint32_t first_page, last_page;
    struct slab *slab = desc->slab;

    _get_position(desc,slot_idx,&first_page,&last_page);

    //If the first page is not shared page, it will be in un-cached state.
    //If the fitst page is a shared page, then I should check the cached state.
    //In a word, I just check the cached state, and needn't check whether the page
    //is a shared page.
    if(bitmap_get_bit(desc->chunk_mem->bitmap,first_page)){
        first_page=UINT32_MAX;
    }
    if(bitmap_get_bit(desc->chunk_mem->bitmap,last_page)){
        last_page=UINT32_MAX;
    }

    if( (first_page==last_page) && (first_page==UINT32_MAX) ){
        //Wonderful! All shared pages has been cached.
        cb(ctx,-KV_ESUCCESS);
        return;
    }

    //Now I have to load the shared pages from disk;
    struct chunk_load_store_ctx* cls_ctx = pool_get(pmgr->load_store_ctx_pool);
    assert(cls_ctx!=NULL);
    
    cls_ctx->pmgr = pmgr;
    cls_ctx->desc = desc;
    cls_ctx->first_page = first_page;
    cls_ctx->last_page = last_page;
    cls_ctx->slot_idx = slot_idx;
    cls_ctx->user_cb = cb;
    cls_ctx->user_ctx = ctx;

    cls_ctx->nb_segs = 0;

    uint8_t *buf;
    uint64_t start_page_in_slab;
    uint64_t nb_pages = 1;

    uint64_t key_prefix = (uint64_t)desc + first_page;

    if(first_page!=UINT32_MAX){
        buf = &desc->chunk_mem->data[first_page*KVS_PAGE_SIZE];
        start_page_in_slab = desc->nb_pages * desc->id + first_page;
        cls_ctx->nb_segs++;
        iomgr_load_pages_async(imgr,slab->blob,key_prefix,buf,
                            start_page_in_slab,nb_pages,
                            _item_share_load_complete_cb_fn,cls_ctx);
    }
    if( (last_page!=first_page) && (last_page!=UINT32_MAX) ){
        // For the item that is stored in single page or of which the last page is cached, it
        // is not be loaded.
        buf = &desc->chunk_mem->data[last_page*KVS_PAGE_SIZE];
        start_page_in_slab = desc->nb_pages * desc->id + last_page;
        cls_ctx->nb_segs++;
        iomgr_load_pages_async(imgr,slab->blob,key_prefix,buf,
                    start_page_in_slab,nb_pages,
                    _item_share_load_complete_cb_fn,cls_ctx);
    }
}

static void
_item_meta_load_complete_cb_fn(void* ctx, int kverrno){
    struct chunk_load_store_ctx* cls_ctx  = ctx;
    struct pagechunk_mgr *pmgr = cls_ctx->pmgr;
    struct chunk_desc *desc = cls_ctx->desc;

    uint32_t first_page  = cls_ctx->first_page;

    if(!kverrno){
        //Load success. Set the cache bits.
        bitmap_set_bit(desc->chunk_mem->bitmap,first_page);
    }
    pool_release(pmgr->load_store_ctx_pool,cls_ctx);
    cls_ctx->user_cb(cls_ctx->user_ctx,kverrno);
}

void pagechunk_load_item_meta_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx){
    uint32_t first_page, last_page;
    struct slab *slab = desc->slab;

    _get_position(desc,slot_idx,&first_page,&last_page);

    //The noval slab placement makes it possible that only one page does the
    //meta data stay in.
    if(bitmap_get_bit(desc->chunk_mem->bitmap,first_page)){
        cb(ctx,-KV_ESUCCESS);
        return;
    }
    struct chunk_load_store_ctx* cls_ctx = pool_get(pmgr->load_store_ctx_pool);
    assert(cls_ctx!=NULL);

    cls_ctx->pmgr = pmgr;
    cls_ctx->desc = desc;
    cls_ctx->first_page = first_page;
    cls_ctx->slot_idx = slot_idx;
    cls_ctx->user_cb = cb;
    cls_ctx->user_ctx = ctx;

    uint8_t *buf = &desc->chunk_mem->data[first_page*KVS_PAGE_SIZE];
    uint64_t start_page_in_slab = desc->nb_pages * desc->id + first_page;
    uint64_t key_prefix = (uint64_t)desc + first_page;

    iomgr_load_pages_async(imgr,slab->blob,key_prefix,buf,start_page_in_slab,1,_item_meta_load_complete_cb_fn,cls_ctx);
}

static void
_item_store_complete_cb_fn(void* ctx, int kverrno){
    struct chunk_load_store_ctx* cls_ctx  = ctx;
    struct pagechunk_mgr *pmgr = cls_ctx->pmgr;
    struct chunk_desc *desc = cls_ctx->desc;

    uint32_t first_page  = cls_ctx->first_page;
    uint32_t last_page = cls_ctx->last_page;
    uint64_t slot_idx = cls_ctx->slot_idx; 

    if(!kverrno){
        if(!pagechunk_is_cached(desc,slot_idx)){
            //User may load only the shared page, so the cache bits shall be set when
            //the item is not wholy cached.
            bitmap_set_bit_range(desc->chunk_mem->bitmap,first_page,last_page);
        }
    }
    pool_release(pmgr->load_store_ctx_pool,cls_ctx);
    cls_ctx->user_cb(cls_ctx->user_ctx,cls_ctx->kverrno);
}

void 
pagechunk_store_item_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx){
    uint32_t first_page, last_page;
    struct slab *slab = desc->slab;

    _get_position(desc,slot_idx,&first_page,&last_page);

    struct chunk_load_store_ctx* cls_ctx = pool_get(pmgr->load_store_ctx_pool);
    assert(cls_ctx!=NULL);

    cls_ctx->pmgr = pmgr;
    cls_ctx->desc = desc;
    cls_ctx->first_page = first_page;
    cls_ctx->slot_idx = slot_idx;
    cls_ctx->user_cb = cb;
    cls_ctx->user_ctx = ctx;

    uint8_t *buf = &desc->chunk_mem->data[first_page*KVS_PAGE_SIZE];
    uint64_t start_page_in_slab = desc->nb_pages * desc->id + first_page;
    uint64_t nb_pages = last_page - first_page + 1;
    uint64_t key_prefix = (uint64_t)desc + first_page;

    iomgr_store_pages_async(imgr,slab->blob,key_prefix,buf,start_page_in_slab,nb_pages,_item_store_complete_cb_fn,cls_ctx);
}

static void
_item_meta_store_complete_cb_fn(void* ctx, int kverrno){
    struct chunk_load_store_ctx* cls_ctx  = ctx;
    struct pagechunk_mgr *pmgr = cls_ctx->pmgr;

    //uint32_t first_page  = cls_ctx->first_page;

    //I needn't set the chunk bitmap, since I have loaded the meta page and set the cache bit
    //in the loading phase.
    pool_release(pmgr->load_store_ctx_pool,cls_ctx);
    cls_ctx->user_cb(cls_ctx->user_ctx,kverrno);
}

void pagechunk_store_item_meta_async(struct pagechunk_mgr *pmgr,
                           struct iomgr* imgr,
                           struct chunk_desc *desc,
                           uint64_t slot_idx,
                           void(*cb)(void*ctx,int kverrno),
                           void* ctx){
    uint32_t first_page, last_page;
    struct slab *slab = desc->slab;

    _get_position(desc,slot_idx,&first_page,&last_page);
    
    struct chunk_load_store_ctx* cls_ctx = pool_get(pmgr->load_store_ctx_pool);
    assert(cls_ctx!=NULL);
    
    cls_ctx->pmgr = pmgr;
    cls_ctx->desc = desc;
    cls_ctx->first_page = first_page;
    cls_ctx->slot_idx = slot_idx;
    cls_ctx->user_cb = cb;
    cls_ctx->user_ctx = ctx;

    uint8_t *buf = &desc->chunk_mem->data[first_page*KVS_PAGE_SIZE];
    uint64_t start_page_in_slab = desc->nb_pages * desc->id + first_page;
    uint64_t key_prefix = (uint64_t)desc + first_page;
    
    iomgr_store_pages_async(imgr,slab->blob,key_prefix,
                            buf,start_page_in_slab,1,
                            _item_meta_store_complete_cb_fn,cls_ctx);
}
/*
bool 
pagechunk_init(uint32_t init_size){
    //todo;
}
*/

struct chunk_mem*
pagechunk_evict_one_chunk(struct pagechunk_mgr *pmgr){
    struct chunk_mem * mem = NULL;
    struct chunk_desc *desc;
    TAILQ_FOREACH_REVERSE(desc,&pmgr->global_chunks,chunk_list_head,link){
        if(!(desc->flag|CHUNK_PIN)){
            mem = desc->chunk_mem;
            desc->chunk_mem=NULL;
            break;
        }
    }
    if(mem){
        //Remove it from the lru list.
        TAILQ_REMOVE(&pmgr->global_chunks,desc,link);
        pmgr->nb_used_chunks--;
    }
    else{
        //I should perform chunk evicting, but now I have no chunks to evict.
        //The nbumber of chunks are too small.
        //This is a system bug.
        SPDK_ERRLOG("Error in chunk request:The number of chunks are too small.");
        assert(0);
    }
    return mem;
}

//This callback shall be exceuted in the original polling thread.
static void _chunk_mem_request_finish(void*ctx){
    struct chunk_miss_callback *cb_obj = ctx;
    struct pagechunk_mgr *pmgr = cb_obj->requestor_pmgr;
    struct chunk_desc *desc = cb_obj->desc;

    int kverrno = cb_obj->kverrno;
    struct chunk_mem* mem = cb_obj->mem;

    if(!kverrno){
        bitmap_clear_bit_all(mem->bitmap);
        desc->chunk_mem = mem;
        TAILQ_INSERT_HEAD(&pmgr->global_chunks,desc,link);
        pmgr->nb_used_chunks++;
    }

    struct chunk_miss_callback *tmp=NULL;

    TAILQ_FOREACH_SAFE(cb_obj,&desc->chunk_miss_callback_head,link,tmp){
        TAILQ_REMOVE(&desc->chunk_miss_callback_head,cb_obj,link);
        pool_release(pmgr->kv_chunk_request_pool,cb_obj);
        cb_obj->cb_fn(cb_obj->ctx,kverrno);
    }
}

void pagechunk_request_one_async(struct pagechunk_mgr *pmgr,
                                 struct chunk_desc* desc,
                                 void(*cb)(void*ctx,int kverrno), 
                                 void* ctx){
    struct chunk_miss_callback *cb_obj = pool_get(pmgr->kv_chunk_request_pool);
    assert(cb_obj!=NULL);
    cb_obj->requestor_pmgr = pmgr;
    cb_obj->desc  = desc;

    cb_obj->finish_cb = _chunk_mem_request_finish;

    cb_obj->cb_fn = cb;
    cb_obj->ctx   = ctx;

    if(!TAILQ_EMPTY(&desc->chunk_miss_callback_head)){
        TAILQ_INSERT_TAIL(&desc->chunk_miss_callback_head,cb_obj,link);
    }   
    else{
        SPDK_NOTICELOG("New chunk mem request\n");
        TAILQ_INSERT_TAIL(&desc->chunk_miss_callback_head,cb_obj,link);
        chunkmgr_request_one_aysnc(cb_obj);
    }
    pmgr->miss_times++;
}

void pagechunk_release_one(struct pagechunk_mgr *pmgr,
                            struct chunk_mem* mem){
    chunkmgr_release_one(pmgr,mem);
}
