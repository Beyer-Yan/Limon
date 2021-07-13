#include <assert.h>
#include "worker_internal.h"
#include "kverrno.h"
#include "spdk/log.h"

static inline void
_resource_release(struct kv_request_internal *req,struct index_entry* entry){
    struct worker_context *wctx = req->pctx.wctx;
    struct process_ctx *pctx = &(req->pctx);

    pool_release(wctx->kv_request_internal_pool,req);
    pagechunk_mem_lower(pctx->desc);
    entry->writing = 0;
}

static void
_process_rmw_write_out_place_store_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct index_entry* new_entry   = &pctx->new_entry;
    
    pagechunk_mem_lower(pctx->new_desc);

    if(kverrno){
        //Store data error, which may be caused by IO error.
        //@todo invalidate the cache state, indicating the update in the cache is invalid.
        //When the slot is read again, it will be loaded from the disk.
        slab_free_slot_async(wctx->rmgr,pctx->slab,new_entry->slot_idx,NULL,NULL);  
    }
    else{
        slab_free_slot_async(wctx->rmgr,pctx->slab,entry->slot_idx,NULL,NULL);
        //Update the entry info with new_entry.
        entry->slot_idx = new_entry->slot_idx;
    }

    req->cb_fn(req->ctx,NULL, kverrno);
    _resource_release(req,entry);
}

static void
_process_rmw_write_out_place_load_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct index_entry* new_entry   = &pctx->new_entry;

    if(kverrno){
        //Load data error, which may be caused by IO error.
        pagechunk_mem_lower(pctx->new_desc);
        _resource_release(req,entry);
        //I have to free the allocated slot
        slab_free_slot_async(wctx->rmgr,pctx->slab,new_entry->slot_idx,NULL,NULL);  
        
        req->cb_fn(req->ctx,NULL,-KV_EIO);
        return;
    }

    //Now I load the item successfully. Next, I will perform writing.
    //Get the modified item.
    struct kv_item *item = pagechunk_get_item(wctx->pmgr,pctx->desc,entry->slot_idx);

    //Put the modified item into another place.
    pagechunk_put_item(wctx->pmgr,pctx->new_desc,new_entry->slot_idx,item);
    pagechunk_store_item_async( wctx->pmgr,
                                wctx->imgr,
                                pctx->new_desc,
                                new_entry->slot_idx,
                                _process_rmw_write_out_place_store_data_cb, 
                                req);
}

static void
_process_rmw_write_out_place_pagechunk_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);
    // Now, load the data from disk for the newly allocated page chunk memory.
    assert(!kverrno);
    pagechunk_load_item_share_async( pctx->wctx->pmgr, 
                               pctx->wctx->imgr, 
                               pctx->new_desc,
                               pctx->new_entry.slot_idx,
                               _process_rmw_write_out_place_load_data_cb,
                               req);
}

static void
_process_rmw_write_out_place_slot_cb(uint64_t slot_idx, void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);
    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    
    if(kverrno){
        //Slot request failed. This may be caused by out-of-disk-space.
        _resource_release(req,entry);
        req->cb_fn(req->ctx,NULL,-KV_EFULL);
        return;
    }

    struct chunk_desc* new_desc = pagechunk_get_desc(pctx->slab,slot_idx);

    assert(new_desc!=NULL);
    pagechunk_mem_lift(new_desc);

    pctx->new_desc = new_desc;
    pctx->new_entry.slot_idx = slot_idx;

    if(!new_desc->chunk_mem){
        //Page chunk is evicted. Now request a new page chunk memory
        pagechunk_request_one_async(wctx->pmgr,new_desc,
                                    _process_rmw_write_out_place_pagechunk_cb,req);
    }else if(pagechunk_is_cached(new_desc,slot_idx)){
        //I needn't load the item from disk, just write it.
        _process_rmw_write_out_place_load_data_cb(req,-KV_ESUCCESS);
    }else{
        //I have to load the pages where the item stays in case I write dirty
        //data for other items that also stay in the same pages.
        pagechunk_load_item_share_async( wctx->pmgr,
                                   wctx->imgr,
                                   new_desc,
                                   slot_idx,
                                   _process_rmw_write_out_place_load_data_cb, 
                                   req);
    }
}

static void
_process_rmw_write_out_place(struct kv_request_internal *req){
    struct process_ctx *pctx    = &req->pctx;
    struct worker_context *wctx = pctx->wctx;

    slab_request_slot_async(wctx->imgr,pctx->slab,_process_rmw_write_out_place_slot_cb,req);
}

static void
_process_rmw_write_in_place_store_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;
    struct index_entry* entry       = pctx->entry;

    _resource_release(req,entry);

    if(kverrno){
        //Error happen, just rollback
        //@TODO Just invalidate the chunk cache ??
    }

    req->cb_fn(req->ctx, NULL, kverrno ? -KV_EIO : -KV_ESUCCESS);
}

static void
_process_rmw_write_in_place(struct kv_request_internal *req){
    struct process_ctx *pctx = &req->pctx;

    pagechunk_store_item_async(pctx->wctx->pmgr,
                               pctx->wctx->imgr,
                               pctx->desc,
                               pctx->entry->slot_idx,
                               _process_rmw_write_in_place_store_cb,
                               req);
}

static void
_process_rmw_get_load_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct chunk_desc *desc         = pctx->desc;

    if(kverrno){
        //Error hits when load data from disk
        req->cb_fn(req->ctx,NULL,-KV_EIO);
        _resource_release(req,entry);
        return;
    }
    else{
        assert(pagechunk_is_cached(desc,entry->slot_idx));
        //Now we load the data into the page chunk cache. Just read it out happily.
        struct kv_item *item = pagechunk_get_item(wctx->pmgr, desc,entry->slot_idx);
        uint32_t size = item_get_size(item);
        int res  = req->m_fn(item);

        if(item_get_size(item)!=size){
            SPDK_ERRLOG("The item size changed in rmw mode\n");
            assert(0);
        }

        //modify_fn returns non-zero val meaning that an error happens
        if(res){
            req->cb_fn(req->ctx,NULL,-KV_EIO);
            _resource_release(req,entry);
            return;
        }
    }
    
    //Perform writing. The item size can not be changed in rmw mode.
    bool is_cross_page = pagechunk_is_cross_page(desc,entry->slot_idx);
    if(is_cross_page){
        //Perform out-of-place writing.
        _process_rmw_write_out_place(req);
    }
    else{
        //Perform in-place writing.
        _process_rmw_write_in_place(req);
    }
}

static void 
_process_rmw_get_pagechunk_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;
    // We do not care the kverrno, because it must success.
    // Now, load the data from disk.
    assert(!kverrno);

    pagechunk_load_item_async(pctx->wctx->pmgr,
                              pctx->wctx->imgr, 
                              pctx->desc,
                              pctx->entry->slot_idx,
                              _process_rmw_get_load_data_cb,
                              req);
}

void worker_process_rmw(struct kv_request_internal *req){
    struct worker_context *wctx = req->pctx.wctx;

    if(!req->pctx.no_lookup){
        req->pctx.entry = mem_index_lookup(wctx->mem_index,req->item);
    }
    
    struct index_entry *entry = req->pctx.entry;

    if(!entry){
        //item does not exist
        pool_release(wctx->kv_request_internal_pool,req);
        req->cb_fn(req->ctx, NULL, -KV_EITEM_NOT_EXIST);
        return;
    }
    else if(entry->writing|entry->deleting){
        //There is already a modifying operation for this item.
        //It should be resubmited.
        //For the entry in deleting state, it has to be re-lookuped, since its
        //entry may be deleted.
        entry->writing ? req->pctx.no_lookup = true : 0;

        TAILQ_INSERT_TAIL(&wctx->resubmit_queue,req,link);
        return;
    }

    req->pctx.slab = &(wctx->shards[entry->shard].slab_set[entry->slab]);
    req->pctx.desc = pagechunk_get_desc(req->pctx.slab,entry->slot_idx);
    struct chunk_desc* desc = req->pctx.desc;
    assert(desc!=NULL);

    //It is a read-modify-write request, treat it as a writinng request.
    entry->writing = 1;
    pagechunk_mem_lift(desc);
    
    if(!desc->chunk_mem){
        //Page chunk is evicted , Now request a new page chunk memory
        pagechunk_request_one_async(wctx->pmgr,desc,_process_rmw_get_pagechunk_cb,req);
    }
    else if(pagechunk_is_cached(desc,entry->slot_idx)){
        //Wonderful! The item is in the page chunk cache.
        _process_rmw_get_load_data_cb(req,-KV_ESUCCESS);
    }
    else{
        //load the data from disk into page chunk
        pagechunk_load_item_async(wctx->pmgr,wctx->imgr, desc,entry->slot_idx,_process_rmw_get_load_data_cb,req);
    }
}
