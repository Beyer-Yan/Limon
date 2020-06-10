#include <assert.h>
#include "worker_internal.h"
#include "kverrno.h"

static void
_process_delete_cb(void*ctx ,int kverrno){
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
    struct chunk_desc *desc = entry->chunk_desc;
    entry->deleting = 1;

    assert(desc!=NULL);

    //Should I wait for the end of reclaiming ??
    uint64_t slot_idx = entry->slot_idx;
    struct slab* slab = desc->slab;
    req->pctx.slab = slab;

    slab_free_slot_async(wctx->rmgr,slab,slot_idx,_process_delete_cb,req);
}

