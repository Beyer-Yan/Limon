#include <assert.h>
#include "worker_internal.h"
#include "kverrno.h"

static void
_process_get_load_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct chunk_desc *desc         = entry->chunk_desc;

    pool_release(wctx->kv_request_internal_pool,req);
    desc->flag &=~ CHUNK_PIN;

    if(kverrno){
        //Error hits when load data from disk
        req->cb_fn(req->ctx,NULL,-KV_EIO);
    }
    else{
        assert(pagechunk_is_cached(desc,entry->slot_idx));
        //Now we load the data into the page chunk cache. Just read it out happily.
        struct kv_item *item = pagechunk_get_item(wctx->pmgr, desc,entry->slot_idx);
        req->cb_fn(req->ctx, item, -KV_ESUCCESS);
    }
}

static void 
_process_get_pagechunk_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;
    // We do not care the kverrno, because it must success.
    // Now, load the data from disk.
    assert(!kverrno);

    pagechunk_load_item_async(pctx->wctx->pmgr,
                              pctx->wctx->imgr, 
                              pctx->entry->chunk_desc,
                              pctx->entry->slot_idx,
                              _process_get_load_data_cb,
                              req);
}

void worker_process_get(struct kv_request_internal *req){
    struct index_entry *entry = req->pctx.entry;
    struct worker_context *wctx = req->pctx.wctx;

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
    struct chunk_desc *desc = entry->chunk_desc;

    assert(desc!=NULL);

    desc->flag |= CHUNK_PIN;
    
    if(!desc->chunk_mem){
        //Page chunk is evicted , Now request a new page chunk memory
        pagechunk_request_one_async(wctx->pmgr,desc,_process_get_pagechunk_cb,req);
    }
    else if(pagechunk_is_cached(desc,entry->slot_idx)){
        //Wonderful! The item is in the page chunk cache.
        _process_get_load_data_cb(req,-KV_ESUCCESS);
    }
    else{
        //load the data from disk into page chunk
        pagechunk_load_item_async(wctx->pmgr,wctx->imgr, desc,entry->slot_idx,_process_get_load_data_cb,req);
    }
}
