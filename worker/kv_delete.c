#include <assert.h>
#include "worker_internal.h"
#include "kverrno.h"

static void
_process_delete_store_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct worker_context *wctx = req->pctx.wctx;
    struct process_ctx *pctx    = &req->pctx;
    struct slab* slab           = pctx->slab;
    struct chunk_desc* desc     = pctx->desc;
    uint64_t slot_idx           = pctx->entry->slot_idx;

    if(kverrno){
        //Error hits when store data to disk
        req->cb_fn(req->ctx,NULL,-KV_EIO);
    }
    else{
        //Now the delete sucesses. And I have to delete it from the memory index
        req->cb_fn(req->ctx,NULL,KV_ESUCCESS);

        mem_index_delete(wctx->global_index,req->item);
        slab_free_slot(wctx->rmgr,slab,slot_idx);
        mtable_release(wctx->mtable,req->sid);
    }

    pagechunk_mem_lower(wctx->pmgr,desc);
    pool_release(wctx->kv_request_internal_pool,req);
}

static void
_process_delete_load_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;

    struct worker_context *wctx   = pctx->wctx;
    struct chunk_desc *desc       = pctx->desc;
    struct slot_entry *entry      = pctx->entry;

    if(kverrno){
        //Error hits when load data from disk
        req->cb_fn(req->ctx,NULL,-KV_EIO);

        pagechunk_mem_lower(wctx->pmgr,desc);
        pool_release(wctx->kv_request_internal_pool,req);
    }
    else{
        //Now we load the data into the page chunk cache. Just read it out happily.
        struct kv_item *item = pagechunk_get_item(wctx->pmgr,desc,entry->slot_idx);
        item->meta.ksize=0;
        pagechunk_store_item_meta_async(wctx->pmgr,wctx->imgr,desc,entry->slot_idx,
                                        _process_delete_store_data_cb,req);
    }
}

void worker_process_delete(struct kv_request_internal *req){
    assert(req);
    assert(req->sid);
    assert(req->item);
    struct worker_context *wctx = req->pctx.wctx;

    if(!mtable_check_valid(wctx->mtable,req->sid)){
        //It may be deleted
        req->cb_fn(req->ctx, NULL, -KV_ESUCCESS);
        pool_release(wctx->kv_request_internal_pool,req);
        return;
    }

    req->pctx.entry = mtable_get(wctx->mtable,req->sid);
    struct slot_entry* entry = req->pctx.entry;

    if(entry->putting || entry->deleting || entry->getting){
        //There is already a modifying operation for this item.
        //It should be resubmited.
        TAILQ_INSERT_TAIL(&wctx->resubmit_queue,req,link);
        return;       
    }

    entry->deleting = 1;
    req->pctx.slab = &(wctx->shards[entry->shard].slab_set[entry->slab]);
    req->pctx.desc = pagechunk_get_desc(req->pctx.slab,entry->slot_idx);
    struct chunk_desc* desc = req->pctx.desc;

    assert(desc!=NULL);
    pagechunk_mem_lift(wctx->pmgr,desc);
    pagechunk_load_item_meta_async(wctx->pmgr,wctx->imgr,desc,entry->slot_idx,
                                   _process_delete_load_data_cb,req);
}

