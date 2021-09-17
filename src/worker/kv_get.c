#include <assert.h>
#include "worker_internal.h"
#include "kverrno.h"

static void
_process_get_load_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;

    struct worker_context *wctx   = pctx->wctx;
    struct slot_entry *entry      = pctx->entry;
    struct chunk_desc *desc       = pctx->desc;

    if(kverrno){
        //Error hits when load data from disk
        req->cb_fn(req->ctx,NULL,-KV_EIO);
    }
    else{
        //Now we load the data into the page chunk cache. Just read it out happily.
        struct kv_item *item = pagechunk_get_item(wctx->pmgr, desc,entry->slot_idx);
        req->cb_fn(req->ctx, item, -KV_ESUCCESS);
    }
    entry->getting = 0;
    pagechunk_mem_lower(wctx->pmgr,desc);
    pool_release(wctx->kv_request_internal_pool,req);
}

void worker_process_get(struct kv_request_internal *req){
    assert(req);
    assert(req->sid);
    struct worker_context *wctx = req->pctx.wctx;

    if(!mtable_check_valid(wctx->mtable,req->sid)){
        //It may be deleted
        req->cb_fn(req->ctx, NULL, -KV_EITEM_NOT_EXIST);
        pool_release(wctx->kv_request_internal_pool,req);
        return;
    }

    req->pctx.entry = mtable_get(wctx->mtable,req->sid);
    struct slot_entry* entry = req->pctx.entry;

    if(entry->putting || entry->deleting){
        //There is already a modifying operation for this item.
        //It should be resubmited.
        TAILQ_INSERT_TAIL(&wctx->resubmit_queue,req,link);
        return;       
    }

    entry->getting = 1;
    req->pctx.slab = &(wctx->shards[entry->shard].slab_set[entry->slab]);
    req->pctx.desc = pagechunk_get_desc(req->pctx.slab,entry->slot_idx);
    struct chunk_desc* desc = req->pctx.desc;

    assert(desc!=NULL);

    pagechunk_mem_lift(wctx->pmgr,desc);
    pagechunk_load_item_async(wctx->pmgr,wctx->imgr,desc,entry->slot_idx,
                              _process_get_load_data_cb,req);
}
