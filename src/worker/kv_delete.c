#include <assert.h>
#include "worker_internal.h"
#include "kverrno.h"

static void
_process_delete_common(void*ctx ,int kverrno){
    struct kv_request_internal *req = ctx;
    struct worker_context *wctx = req->pctx.wctx;

    if(kverrno){
        //Delete failed because of IO error
        req->cb_fn(req->ctx,NULL,-KV_EIO);
        req->pctx.entry->deleting = 0;
    }
    else{
        //Now the delete sucesses. And I have to delete it from the memory index
        req->cb_fn(req->ctx,NULL,KV_ESUCCESS);
        mem_index_delete(wctx->mem_index,req->item);
    }
    pool_release(wctx->kv_request_internal_pool,req);
}


static void
_process_delete_store_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct worker_context *wctx = req->pctx.wctx;
    struct slab* slab           = req->pctx.slab;
    struct chunk_desc* desc = req->pctx.desc;
    uint64_t slot_idx = req->pctx.entry->slot_idx;
    
    pagechunk_mem_lower(desc);
    slab_free_slot(wctx->rmgr,slab,slot_idx);

    _process_delete_common(req,kverrno);
}

static void
_process_delete_load_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct worker_context *wctx = req->pctx.wctx;
    struct chunk_desc* desc = req->pctx.desc;
    uint64_t slot_idx = req->pctx.entry->slot_idx;

    if(kverrno){
        //Error hits when load data from disk
        pagechunk_mem_lower(desc);
        _process_delete_common(ctx,kverrno);
        return;
    }
    struct kv_item *item = pagechunk_get_item(wctx->pmgr,desc,slot_idx);
    item->meta.ksize=0;
    pagechunk_store_item_meta_async(wctx->pmgr,wctx->imgr,desc,slot_idx,
                            _process_delete_store_data_cb,req);
}

static void 
_process_delete_pagechunk_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct worker_context *wctx = req->pctx.wctx;
    struct chunk_desc* desc = req->pctx.desc;
    uint64_t slot_idx = req->pctx.entry->slot_idx;
    // We do not care the kverrno, because it must success.
    // Now, load the data from disk.
    assert(!kverrno);

    pagechunk_load_item_meta_async(wctx->pmgr,wctx->imgr, 
                                    desc,slot_idx,
                                    _process_delete_load_data_cb,req);
}

void worker_process_delete(struct kv_request_internal *req){
    struct worker_context *wctx = req->pctx.wctx;

    if(!req->pctx.no_lookup){
        req->pctx.entry = mem_index_lookup(wctx->mem_index,req->item);
    }
    
    struct index_entry *entry = req->pctx.entry;

    if(!entry){
        //item does not exist
        req->cb_fn(req->ctx, NULL, -KV_EITEM_NOT_EXIST);
        pool_release(wctx->kv_request_internal_pool,req);
        return;
    }
    else if(entry->writing | entry->deleting | entry->getting){
        //There is already a getting or modifying operation for this item.
        //It should be resubmited.
        //For the entry in deleting state, it has to be re-lookuped, since its
        //entry may be deleted.
        (entry->writing|entry->getting) ? req->pctx.no_lookup = true : 0;
        
        TAILQ_INSERT_TAIL(&wctx->resubmit_queue,req,link);
        return;
    }

    entry->deleting = 1;

    req->pctx.entry = entry;
    req->pctx.slab = &(wctx->shards[entry->shard].slab_set[entry->slab]);
    req->pctx.desc = pagechunk_get_desc(req->pctx.slab,entry->slot_idx);
    struct chunk_desc* desc = req->pctx.desc;
    assert(desc!=NULL);

    if(!desc->chunk_mem){
        pagechunk_request_one_async(wctx->pmgr,desc,
                                    _process_delete_pagechunk_cb,req);
    }
    else if(pagechunk_is_cached(desc,entry->slot_idx)){
        _process_delete_load_data_cb(req,-KV_ESUCCESS);
    }
    else{
        pagechunk_load_item_meta_async(wctx->pmgr,wctx->imgr,
                                  desc,entry->slot_idx,
                                  _process_delete_load_data_cb,req);
    }
}

