#include <assert.h>
#include "worker_internal.h"
#include "kverrno.h"

#include "spdk/log.h"

/*----------------------------------------------------------*/
//Here are common functions
//Nothind currently

/*----------------------------------------------------------*/

static void
_process_put_case1_store_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct index_entry* new_entry   = &pctx->new_entry;

    struct slab* slab = pctx->slab_changed ? pctx->new_slab : pctx->slab;
    
    pool_release(wctx->kv_request_internal_pool,req);
    pagechunk_mem_lower(pctx->desc);
    pagechunk_mem_lower(pctx->new_desc);

    if(kverrno){
        //Store data error, which may be caused by IO error.
        //the update is out-of-place, so the cache state is not needed to be invalidated.
        slab_free_slot_async(wctx->rmgr,slab,new_entry->slot_idx,NULL,NULL);  
    }
    else{
        //Now I have to reclaim the old slot index. But it is posted to 
        //background stage
        slab_free_slot_async(wctx->rmgr,pctx->slab,entry->slot_idx,NULL,NULL);

        //Update the entry info with new_entry.
        *entry = pctx->new_entry;
    }

    req->cb_fn(req->ctx,NULL, kverrno ? -KV_EIO : -KV_ESUCCESS);
}

static void
_process_put_case1_load_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct index_entry* new_entry   = &pctx->new_entry;

    struct slab* slab = pctx->slab_changed ? pctx->new_slab : pctx->slab;

    if(kverrno){
        //Load data error, which may be caused by IO error.
        pool_release(wctx->kv_request_internal_pool,req);
        pagechunk_mem_lower(pctx->desc);
        pagechunk_mem_lower(pctx->new_desc);
        entry->writing = 0;
        //I have to free the allocated slot
        slab_free_slot_async(wctx->rmgr,slab,new_entry->slot_idx,NULL,NULL);  
        
        req->cb_fn(req->ctx,NULL,-KV_EIO);
        return;
    }

    //Now I load the item successfully. Next, I will perform writing.
    pagechunk_put_item(wctx->pmgr,pctx->new_desc,new_entry->slot_idx,req->item);
    pagechunk_store_item_async( wctx->pmgr,
                                wctx->imgr,
                                pctx->new_desc,
                                new_entry->slot_idx,
                                _process_put_case1_store_data_cb, 
                                req);
}

static void _process_put_case1_pagechunk_request_cb(void* ctx, int kverrno){

    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);
    // Now, load the data from disk for the newly allocated page chunk memory.
    assert(!kverrno);
    pagechunk_load_item_share_async( pctx->wctx->pmgr, 
                               pctx->wctx->imgr, 
                               pctx->new_desc,
                               pctx->new_entry.slot_idx,
                               _process_put_case1_load_data_cb,
                               req);

}

static void 
_process_put_case1_request_slot_cb(uint64_t slot_idx, void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;

    struct slab* slab = pctx->slab_changed ? pctx->new_slab : pctx->slab;
    
    if(kverrno){
        //Slot request failed. This may be caused by out-of-disk-space.
        pool_release(wctx->kv_request_internal_pool,req);
        entry->writing = 0;
        pagechunk_mem_lower(pctx->desc);
        req->cb_fn(req->ctx,NULL,-KV_EFULL);
        return;
    }

    struct chunk_desc* new_desc = pagechunk_get_desc(slab,slot_idx);
    assert(new_desc!=NULL);

    pctx->new_entry.slot_idx = slot_idx;
    pctx->new_desc = new_desc;
    pagechunk_mem_lift(new_desc);

    if(!new_desc->chunk_mem){
        //Page chunk is evicted. Now request a new page chunk memory
        pagechunk_request_one_async(wctx->pmgr,new_desc,
                                    _process_put_case1_pagechunk_request_cb,req);
    }else if(pagechunk_is_cached(new_desc,slot_idx)){
        //I needn't load the item from disk, just write it.
        _process_put_case1_load_data_cb(req,-KV_ESUCCESS);
    }else{
        //I have to load the pages where the item stays in case I write dirty
        //data for other items that also stay in the same pages.
        pagechunk_load_item_share_async( wctx->pmgr,
                                   wctx->imgr,
                                   new_desc,
                                   slot_idx,
                                   _process_put_case1_load_data_cb, 
                                   req);
    }
}

static void 
_process_put_case1(struct kv_request_internal *req, bool slab_changed){
    struct process_ctx *pctx = &(req->pctx);
    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct slab* slab = pctx->slab;

    pctx->new_entry = * pctx->entry;

    if(slab_changed){
        uint32_t new_slab_idx = slab_find_slab(item_packed_size(req->item));

        pctx->slab_changed    = 1;
        pctx->new_entry.slab  = new_slab_idx;
        pctx->new_entry.shard = entry->shard;
        pctx->new_slab = &(wctx->shards[entry->shard].slab_set[new_slab_idx]);

        slab = pctx->new_slab;
        //SPDK_NOTICELOG("slab changed, ori_slab:%u, new_slab:%u\n",pctx->old_slab->slab_size, pctx->slab->slab_size);
    }
    slab_request_slot_async(wctx->imgr,slab,
                            _process_put_case1_request_slot_cb,
                            req);
}

/*-------------------------------------------------------------*/

static void
_process_put_case2_store_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct chunk_desc *desc         = pctx->desc;

    pool_release(wctx->kv_request_internal_pool,req);
    entry->writing = 0;
    pagechunk_mem_lower(desc);

    if(kverrno){
        //@todo invalidate the cache state, indicating the update is invalid.
    }

    req->cb_fn(req->ctx, NULL, kverrno ? -KV_EIO : -KV_ESUCCESS);
}

static void
_process_put_case2_cached(struct kv_request_internal *req){

    struct process_ctx *pctx = &req->pctx;

    pagechunk_put_item(pctx->wctx->pmgr,
                       pctx->desc, 
                       pctx->entry->slot_idx,
                       req->item);

    pagechunk_store_item_async(pctx->wctx->pmgr,
                               pctx->wctx->imgr,
                               pctx->desc,
                               pctx->entry->slot_idx,
                               _process_put_case2_store_data_cb,
                               req);
}

static void
_process_put_case2_load_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct chunk_desc *desc         = pctx->desc;

    if(kverrno){
        //Error hits when load data from disk
        pool_release(wctx->kv_request_internal_pool,req);
        entry->writing = 0;
        pagechunk_mem_lower(desc);
        
        req->cb_fn(req->ctx,NULL,-KV_EIO);
        return;
    } 
    _process_put_case2_cached(req);
}

static void 
_process_put_case2_pagechunk_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;
    // Now, load the data from disk.
    assert(!kverrno);

    pagechunk_load_item_share_async(pctx->wctx->pmgr,
                              pctx->wctx->imgr, 
                              pctx->desc,
                              pctx->entry->slot_idx,
                              _process_put_case2_load_data_cb,
                              req);
}

static void
_process_put_case2(struct kv_request_internal *req){
    struct process_ctx *pctx = &(req->pctx);
    struct worker_context *wctx     = pctx->wctx;
    struct index_entry *entry = req->pctx.entry;
    struct chunk_desc *desc   = pctx->desc;

    // Now I know that the item is stored into single page. So if the page chunk
    // is evicted, I have to get a new page chunk memory.
    if(!desc->chunk_mem){
        //Page chunk is evicted , Now request a new page chunk memory
        pagechunk_request_one_async(wctx->pmgr,desc,_process_put_case2_pagechunk_cb,req);
    }
    else if(pagechunk_is_cached(desc,entry->slot_idx)){
        //The item is cached and stored in single page, just update in place
        _process_put_case2_cached(req);
    }
    else{
        //The item is stored into a single page but not cached, so I have to load 
        //the data into page chunk cache before I perform update
        pagechunk_load_item_share_async(wctx->pmgr,wctx->imgr, desc,entry->slot_idx,
                                    _process_put_case2_load_data_cb,req);
    }
}


/*-------------------------------------------------------------*/
static void
_process_put_case3_store_data_cb(void*ctx, int kverrno){
    
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;

    if(kverrno){
        //Store data error, which may be caused by IO error.
        //The newly inseerted index shall be deleted.
        slab_free_slot_async(wctx->rmgr,pctx->slab,entry->slot_idx,NULL,NULL);
        mem_index_delete(wctx->mem_index,req->item);    
    }
    else{
        entry->writing = 0;
    }

    pool_release(wctx->kv_request_internal_pool,req);
    pagechunk_mem_lower(pctx->desc);

    req->cb_fn(req->ctx,NULL,kverrno ? -KV_EIO : -KV_ESUCCESS);
}

static void
_process_put_case3_load_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;

    if(kverrno){
        //Load data error, which may be caused by IO error.
        pool_release(wctx->kv_request_internal_pool,req);
        pagechunk_mem_lower(pctx->desc);
        //I have to free the allocated slot in background
        slab_free_slot_async(wctx->rmgr,pctx->slab,entry->slot_idx,NULL,NULL);
        mem_index_delete(wctx->mem_index,req->item); 

        req->cb_fn(req->ctx,NULL,-KV_EIO); 
        return;  
    }

    //SPDK_NOTICELOG("Put add new load data completes, storing key:%d\n",*(int*)req->item->data);

    //Now I load the item successfully. Next, I will perform writing.
    pagechunk_put_item(wctx->pmgr,pctx->desc,entry->slot_idx,req->item);
    pagechunk_store_item_async( wctx->pmgr,
                                wctx->imgr,
                                pctx->desc,
                                entry->slot_idx,
                                _process_put_case3_store_data_cb, 
                                req);
}

static void 
_process_put_case3_pagechunk_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    // Now, load the data from disk.
    assert(!kverrno);
    pagechunk_load_item_share_async( pctx->wctx->pmgr,
                               pctx->wctx->imgr, 
                               pctx->desc,
                               pctx->entry->slot_idx,
                               _process_put_case3_load_data_cb,
                               req);
}

static void
_process_put_case3_request_slot_cb(uint64_t slot_idx, void* ctx, int kverrno){
    
    //SPDK_NOTICELOG("Put add new slot request, slot:%lu\n",slot_idx);
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    if(kverrno){
        //Slot request failed. This may be caused by out-of-disk-space.
        //Adding item failed.
        SPDK_ERRLOG("failed in slot allocating, slot:%u, err:%d\n",pctx->slab->slab_size, kverrno);
        pool_release(wctx->kv_request_internal_pool,req);
        mem_index_delete(wctx->mem_index,req->item);
        req->cb_fn(req->ctx,NULL,-KV_EFULL);
        return;
    }
    struct chunk_desc* desc = pagechunk_get_desc(pctx->slab,slot_idx);

    assert(desc!=NULL);
    pagechunk_mem_lift(desc);
    pctx->desc = desc;
    entry->slot_idx = slot_idx;

    if(!desc->chunk_mem){
        //Page chunk is evicted. Now request a new page chunk memory
        pagechunk_request_one_async(wctx->pmgr,desc,_process_put_case3_pagechunk_cb,req);
    }else if(pagechunk_is_cached(desc,slot_idx)){
        //I needn't load the item from disk, just write it.
        _process_put_case3_load_data_cb(req,-KV_ESUCCESS);
    }else{
        //I have to load the pages where the item stays in case I write dirty
        //data for other items that also stay in the same pages.
        pagechunk_load_item_share_async(wctx->pmgr,
                                  wctx->imgr,
                                  desc,
                                  slot_idx,
                                  _process_put_case3_load_data_cb, 
                                  req);
    }
}

static void
_process_put_case3(struct kv_request_internal *req){

    struct worker_context *wctx = req->pctx.wctx;

    struct index_entry new_entry = {0};
    new_entry.writing = 1;
    
    /**
     * @brief The entry will be copied into memory index. So I can pass the stack 
     * variable here. Add the item into the memory index to prevent the subsequent
     * put operation for the same item. If a put request for this item is processed,
     * it will be resubmited into resubmit queue because of the writing status.
     */
    struct index_entry *entry = mem_index_add(wctx->mem_index,req->item,&new_entry);
    if(!entry){
        //The entry does not exist in memory index, but it returns an error.
        //So it is the OOM! Just return memory error.
        pool_release(wctx->kv_request_internal_pool,req);
        req->cb_fn(req->ctx,NULL,-KV_EMEM);
        return;
    }
    uint32_t shard_idx = req->shard;
    uint32_t slab_idx = slab_find_slab(item_packed_size(req->item));
    entry->shard = shard_idx;
    entry->slab = slab_idx;

    struct slab* slab = &(wctx->shards[shard_idx].slab_set[slab_idx]);
    req->pctx.slab = slab;
    req->pctx.entry = entry;

    //SPDK_NOTICELOG("Get slab from shard:%u, slab size:%u\n",req->shard,slab->slab_size);

    slab_request_slot_async(wctx->imgr,slab,_process_put_case3_request_slot_cb,req);
}

/*--------------------------------------------------------------------*/

/**
 * @brief put case1 : for an item that is updated not in place. The case is designed to 
 *                    process items that are stored across pages or the size of the slab
 *                    for the item is changed. Under such case, the item shall be stored
 *                    into another slot, and the original data is marked as "delete" and 
 *                    posted to the background reclaim manager.
 * 
 *        put case2 : for an item that is updated in place. The case is designed to process
 *                    items that are stored in a page. Under such case, the item is updated
 *                    directly in place. No extra reclaiming work shall be performed.
 *        
 *        put case3 : for the new item. I need request a new slot and insert it into the mem
 *                    index.
 * 
 * @param req The kv_request_internal object
 */
void worker_process_put(struct kv_request_internal *req){
    struct worker_context *wctx = req->pctx.wctx;

    if(!req->pctx.no_lookup){
        req->pctx.entry = mem_index_lookup(wctx->mem_index,req->item);
    }
    
    struct index_entry *entry = req->pctx.entry;

    if(!entry){
        //item does not exist. It a new item!
        _process_put_case3(req);
        return;
    }
    if(entry->writing | entry->deleting){
        //There is already a modifying operation for this item.
        //It should wait.
        //For the entry in deleting state, it has to be re-lookuped, since its
        //entry may be deleted.
        entry->writing ? req->pctx.no_lookup = true : 0;
        TAILQ_INSERT_TAIL(&wctx->resubmit_queue,req,link);
        return;
    }

    // replace an existing item
    req->pctx.slab = &(wctx->shards[entry->shard].slab_set[entry->slab]);
    req->pctx.desc = pagechunk_get_desc(req->pctx.slab,entry->slot_idx);
    struct chunk_desc* desc = req->pctx.desc;
    assert(desc!=NULL);
    
    bool slab_changed = slab_is_slab_changed(desc->slab_size,item_packed_size(req->item));
    bool is_cross_page = pagechunk_is_cross_page(desc,entry->slot_idx);

    if(entry->getting){
        if(slab_changed|is_cross_page){
            //There is a pending getting request, but the puting will be performed
            //not-in-place, that is, I will not load the item data and store the item
            //directly into other place and change the index entry. If the pending
            //getting request is not finished before I change the index entry, the 
            //getting request will encounter race trouble.
            //e.g. get: issue         load data              load ok          finished
            //     put         issue             change entry        (race here)      

            //If the item is stored in-place, then it will be loaded brfore I perform
            //writing. That is, Other flying getting request will be finished before
            //I perform writing. In a word, in such case, it is unneccesary to care the 
            //getting flag. That is an opimization measure to speed up the putting and 
            //getting for small items.
            req->pctx.no_lookup = true;
            TAILQ_INSERT_TAIL(&wctx->resubmit_queue,req,link);  
            return;
        }
    }

    entry->writing = 1;
    pagechunk_mem_lift(desc);

    if(slab_changed){
        //The updated item is not situable for the slab, I have to write it
        //into other slab. So I needn't load it and I do not pay 
        //attention to page chunk evicting. Just add the item into other place.
        _process_put_case1(req,true);
    }
    else if(is_cross_page){
        //The item is stored into multi page, so I needn't
        //load it and I do not pay attention to page chunk evicting.
        //Just add the item into other place.
        _process_put_case1(req,false);
    }
    else{
        _process_put_case2(req);
    }
}
