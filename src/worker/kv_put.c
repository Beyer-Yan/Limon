#include <assert.h>
#include "worker_internal.h"
#include "kverrno.h"

#include "spdk/log.h"

/*----------------------------------------------------------*/

static void
process_put_outplace_store_meta_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct worker_context *wctx = req->pctx.wctx;
    struct process_ctx *pctx    = &req->pctx;
    struct slab* slab           = pctx->slab;
    struct chunk_desc* desc     = pctx->desc;
    uint64_t old_slot_idx       = pctx->new_entry.slot_idx;

    if(kverrno){
        SPDK_WARNLOG("Failed to write tombstone, shard:%u,slab:%u,slot:%lu\n",
                      req->shard,slab->slab_size,old_slot_idx);
    }
    pctx->phase = 5;
    //Just release the old slot
    slab_free_slot(wctx->rmgr,slab,old_slot_idx);
    pagechunk_mem_lower(wctx->pmgr,desc);
    pool_release(wctx->kv_request_internal_pool,req);
}

static void
process_put_outplace_load_meta_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;
    struct worker_context *wctx   = pctx->wctx;
    struct chunk_desc *desc       = pctx->desc;
    uint64_t old_slot_idx         = pctx->new_entry.slot_idx;

    if(kverrno){
        //Error hits when load data from disk. But it is OK
        //since the old one will be dropped in recovery.
        SPDK_WARNLOG("Failed to load meta page, shard:%u,slab:%u,slot:%lu\n",
                      req->shard,pctx->slab->slab_size,old_slot_idx);
        
        //Just release the old slot
        slab_free_slot(wctx->rmgr,pctx->slab,old_slot_idx);
        pagechunk_mem_lower(wctx->pmgr,desc);
        pool_release(wctx->kv_request_internal_pool,req);
    }
    else{
        //Now we load the data into the page chunk cache. Just read it out happily.
        struct kv_item *item = pagechunk_get_item(wctx->pmgr, desc,old_slot_idx);
        item->meta.ksize=0;
        pctx->phase = 4;
        pagechunk_store_item_meta_async(wctx->pmgr,wctx->imgr,desc,old_slot_idx,
                                        process_put_outplace_store_meta_cb,req);
    }
}

static void
_process_put_outplace_store_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);
    struct worker_context *wctx     = pctx->wctx;
    struct slab* slab = pctx->slab_changed ? pctx->new_slab : pctx->slab;
    struct slot_entry  *entry       = pctx->entry;

    if(kverrno){
        //Store data error, which may be caused by IO error.
        req->cb_fn(req->ctx,NULL,-KV_EIO);

        entry->putting = 0;
        slab_free_slot(wctx->rmgr,slab,pctx->new_entry.slot_idx);  
        pagechunk_mem_lower(wctx->pmgr, pctx->new_desc); 
        pool_release(wctx->kv_request_internal_pool,req);
        return;
    }

    //Update sucess
    req->cb_fn(req->ctx, NULL, -KV_ESUCCESS);

    uint64_t old_slot = entry->slot_idx;
    uint64_t old_slab = pctx->slab;

    entry->slot_idx = pctx->new_entry.slot_idx;
    entry->slab = pctx->new_entry.slab;
    entry->putting = 0;

    pagechunk_mem_lower(wctx->pmgr,pctx->new_desc);  

    //Free old slot.
    slab_free_slot(wctx->rmgr,old_slab,old_slot);
    pool_release(wctx->kv_request_internal_pool,req);
    return;

    //Writing meta may not nessacery since the old slot will be 
    //reclaimed in recovery.
    //Now update the slot entry and reclaim the old slot
    /*
    struct slot_entry old_entry = *entry;

    //update the slot entry.
    entry->slot_idx = pctx->new_entry.slot_idx;
    entry->slab = pctx->new_entry.slab;
    entry->putting = 0;

    //temporarily record the old entry with new_entry field.
    pctx->new_entry = old_entry;
    
    pctx->phase = 3;
    //Then write the tombstone to the old slot
    pagechunk_mem_lift(wctx->pmgr,pctx->desc);
    pagechunk_load_item_meta_async(wctx->pmgr,wctx->imgr,
                                   pctx->desc,old_entry.slot_idx,
                                   process_put_outplace_load_meta_cb,req);
    */
}

static void
_process_put_outplace_load_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);
    struct worker_context *wctx     = pctx->wctx;
    struct slab* slab = pctx->slab_changed ? pctx->new_slab : pctx->slab;
    struct slot_entry  *entry       = pctx->entry;

    if(kverrno){
        //Load data error, which may be caused by IO error.
        req->cb_fn(req->ctx,NULL,-KV_EIO);

        entry->putting = 0;
        slab_free_slot(wctx->rmgr,slab,pctx->new_entry.slot_idx); 
        pagechunk_mem_lower(wctx->pmgr, pctx->new_desc); 
        pool_release(wctx->kv_request_internal_pool,req);
        return;
    }

    pctx->phase = 2;
    //Now I load the item successfully. Next, I will perform writing.
    pagechunk_put_item(wctx->pmgr,pctx->new_desc,pctx->new_entry.slot_idx,req->item);
    pagechunk_store_item_async( wctx->pmgr,
                                wctx->imgr,
                                pctx->new_desc,
                                pctx->new_entry.slot_idx,
                                _process_put_outplace_store_data_cb, 
                                req);
}

static void 
_process_put_outplace_request_slot_cb(uint64_t slot_idx, void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);
    struct worker_context *wctx   = pctx->wctx;
    struct slab* slab = pctx->slab_changed ? pctx->new_slab : pctx->slab;
    struct slot_entry* entry      = pctx->entry;
    
    if(kverrno){
        //Slot request failed. This may be caused by out-of-disk-space.
        req->cb_fn(req->ctx,NULL,-KV_EFULL);
        
        entry->putting = 0;
        pool_release(wctx->kv_request_internal_pool,req);
        return;
    }

    struct chunk_desc* new_desc = pagechunk_get_desc(slab,slot_idx);
    assert(new_desc!=NULL);

    pctx->new_entry.slot_idx = slot_idx;
    pctx->new_desc = new_desc;

    pagechunk_mem_lift(wctx->pmgr,new_desc);
    pctx->phase = 1;
    pagechunk_load_item_share_async( wctx->pmgr,
                                   wctx->imgr,
                                   new_desc,
                                   slot_idx,
                                   _process_put_outplace_load_data_cb, 
                                   req);
}

static void 
_process_put_outplace(struct kv_request_internal *req, bool slab_changed){
    struct process_ctx *pctx = &(req->pctx);
    struct worker_context *wctx  = pctx->wctx;
    struct slab* slab            = pctx->slab;

    if(slab_changed){
        uint32_t new_slab_idx = slab_find_slab(item_packed_size(req->item));
        pctx->slab_changed    = 1;

        pctx->new_entry.raw = 0;
        pctx->new_entry.slab  = new_slab_idx;
        pctx->new_entry.shard = req->shard;
        pctx->new_slab = &(wctx->shards[req->shard].slab_set[new_slab_idx]);

        slab = pctx->new_slab;
    }
    else{
        //It is just an out-of-place, we allocate new slot from the same slab.
        pctx->new_entry.raw = 0;
        pctx->new_entry.slab = pctx->entry->slab;
        pctx->new_entry.shard = pctx->entry->shard;
    }
    slab_request_slot_async(wctx->imgr,slab,
                            _process_put_outplace_request_slot_cb,
                            req);
}

/*-------------------------------------------------------------*/

static void
_process_put_inplace_store_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;
    struct worker_context *wctx   = pctx->wctx;
    struct chunk_desc *desc       = pctx->desc;
    struct slot_entry* entry      = pctx->entry;

    if(kverrno){
        //@TODO invalidate the cache state, indicating the update is invalid.
    }

    req->cb_fn(req->ctx, NULL, kverrno ? -KV_EIO : -KV_ESUCCESS);
    
    entry->putting = 0;
    pagechunk_mem_lower(wctx->pmgr,desc);
    pool_release(wctx->kv_request_internal_pool,req); 
}

static void
_process_put_inplace_load_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;
    struct worker_context *wctx  = pctx->wctx;
    struct chunk_desc *desc      = pctx->desc;
    struct slot_entry* entry     = pctx->entry;

    if(kverrno){
        //Error hits when load data from disk        
        req->cb_fn(req->ctx,NULL,-KV_EIO);

        entry->putting = 0;
        pagechunk_mem_lower(wctx->pmgr,pctx->desc);
        pool_release(wctx->kv_request_internal_pool,req);
        return;
    }

    pagechunk_put_item(wctx->pmgr,pctx->desc, entry->slot_idx,req->item);
    pagechunk_store_item_async(wctx->pmgr,wctx->imgr,
                               pctx->desc,entry->slot_idx,
                               _process_put_inplace_store_data_cb,
                               req); 
}

static void
_process_put_inplace(struct kv_request_internal *req){
    struct process_ctx *pctx = &(req->pctx);
    struct worker_context *wctx     = pctx->wctx;
    struct chunk_desc *desc   = pctx->desc;
    struct slot_entry *entry  = req->pctx.entry;

    pagechunk_mem_lift(wctx->pmgr,desc);
    // Now I know that the item is stored into single page.
    pagechunk_load_item_async(wctx->pmgr,wctx->imgr,desc,entry->slot_idx,
                              _process_put_inplace_load_data_cb,req);
}

/*-------------------------------------------------------------*/
static void
_process_put_add_store_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);
    struct worker_context *wctx   = pctx->wctx;
    struct slot_entry entry       = pctx->new_entry;

    if(kverrno){
        //Store data error, which may be caused by IO error.
        //The newly inserted index shall be deleted.
        slab_free_slot(wctx->rmgr,pctx->new_slab,entry.slot_idx);
    }
    else{
        //add it into mtable and global index
        uint64_t sid = mtable_alloc_sid(wctx->mtable,entry);
        assert(sid);
        mem_index_add(wctx->global_index,req->item,sid);
    }

    req->cb_fn(req->ctx,NULL,kverrno ? -KV_EIO : -KV_ESUCCESS);

    conflict_leave(wctx->add_conflicts,pctx->conflict_bucket,req->op_code);
    pagechunk_mem_lower(wctx->pmgr,pctx->new_desc);
    pool_release(wctx->kv_request_internal_pool,req); 
}

static void
_process_put_add_load_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx        = &(req->pctx);
    struct worker_context *wctx  = pctx->wctx;
    struct slot_entry entry = pctx->new_entry;

    if(kverrno){
        //Load data error, which may be caused by IO error.
        req->cb_fn(req->ctx,NULL,-KV_EIO); 
        
        slab_free_slot(wctx->rmgr,pctx->new_slab,entry.slot_idx);
        conflict_leave(wctx->add_conflicts,pctx->conflict_bucket,req->op_code);
        pagechunk_mem_lower(wctx->pmgr,pctx->new_desc);
        pool_release(wctx->kv_request_internal_pool,req);
        return;  
    }

    //SPDK_NOTICELOG("Put add new load data completes, storing key:%d\n",*(int*)req->item->data);
    //Now I load the item successfully. Next, I will perform writing.
    pagechunk_put_item(wctx->pmgr,pctx->new_desc,entry.slot_idx,req->item);
    pagechunk_store_item_async( wctx->pmgr,
                                wctx->imgr,
                                pctx->new_desc,
                                entry.slot_idx,
                                _process_put_add_store_data_cb, 
                                req);
}

static void
_process_put_add_request_slot_cb(uint64_t slot_idx, void* ctx, int kverrno){
    
    //SPDK_NOTICELOG("Put add new slot request, slot:%lu\n",slot_idx);
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx        = &(req->pctx);
    struct worker_context *wctx     = pctx->wctx;

    if(kverrno){
        //Slot request failed. This may be caused by out-of-disk-space.
        SPDK_ERRLOG("failed in slot allocating, slot:%u, err:%d\n",
                     pctx->slab->slab_size, kverrno);
        req->cb_fn(req->ctx,NULL,-KV_EFULL);
        
        conflict_leave(wctx->add_conflicts,pctx->conflict_bucket,req->op_code);
        pool_release(wctx->kv_request_internal_pool,req);
        return;
    }

    struct chunk_desc* desc = pagechunk_get_desc(pctx->new_slab,slot_idx);
    assert(desc!=NULL);

    pagechunk_mem_lift(wctx->pmgr,desc);
    pctx->new_desc = desc;
    pctx->new_entry.slot_idx = slot_idx;

    pagechunk_load_item_async(wctx->pmgr,wctx->imgr,desc,slot_idx,
                              _process_put_add_load_data_cb,req);
}   

static void
_process_put_add(struct kv_request_internal *req){
    struct worker_context *wctx = req->pctx.wctx;

    uint32_t bucket = conflict_check_or_enter(wctx->add_conflicts,req->item,req->op_code);
    if(!bucket){
        //Someone is adding the same item, retry.
        TAILQ_INSERT_TAIL(&wctx->resubmit_queue,req,link);
        return;
    };

    //check whether the item has been added into global index
    uint64_t sid = mem_index_lookup(wctx->global_index,req->item);
    if(sid){
        //The item has been added, So drop it.
        //User should resolve the race.
        req->cb_fn(req->ctx,NULL,-KV_EOP_RACE);
        conflict_leave(wctx->add_conflicts,bucket,req->op_code);
        pool_release(wctx->kv_request_internal_pool,req);
        return;
    }
    req->pctx.conflict_bucket = bucket;

    uint32_t shard_idx = req->shard;
    uint32_t slab_idx = slab_find_slab(item_packed_size(req->item));
    struct slab* slab = &(wctx->shards[shard_idx].slab_set[slab_idx]);
    req->pctx.new_slab = slab;

    req->pctx.new_entry.raw = 0;
    req->pctx.new_entry.shard = shard_idx;
    req->pctx.new_entry.slab = slab_idx;

    //SPDK_NOTICELOG("Get slab from shard:%u, slab size:%u\n",req->shard,slab->slab_size);

    slab_request_slot_async(wctx->imgr,slab,_process_put_add_request_slot_cb,req);
}

/*--------------------------------------------------------------------*/

/**
 * @brief put update_outplace : for an item that is updated not in place. The case is designed to 
 *                    process items that are stored across pages or the size of the slab
 *                    for the item is changed. Under such case, the item shall be stored
 *                    into another slot, and the original data is marked as "delete" and 
 *                    posted to the background reclaim manager.
 * 
 *        put update_inplace : for an item that is updated in place. The case is designed to process
 *                    items that are stored in a page. Under such case, the item is updated
 *                    directly in place. No extra reclaiming work shall be performed.
 *        
 *        put add   : for the new item. I need request a new slot and insert it into the mem
 *                    index.
 * 
 * @param req The kv_request_internal object
 */
void worker_process_put(struct kv_request_internal *req){
    assert(req);
    struct worker_context *wctx = req->pctx.wctx;

    if(!req->sid || !mtable_check_valid(wctx->mtable,req->sid)){
        //item does not exist for 0 sid. It a new item!
        //If the sid is valid, but it is deleted, then just 
        //treat it as a new item
        _process_put_add(req);
        return;
    }

    //It is an update
    req->pctx.entry = mtable_get(wctx->mtable,req->sid);
    struct slot_entry* entry = req->pctx.entry;

    if(entry->putting || entry->deleting || entry->getting){
        //There is already an operation for this item.
        //It should be resubmited.
        TAILQ_INSERT_TAIL(&wctx->resubmit_queue,req,link);
        return;       
    }

    req->pctx.slab = &(wctx->shards[entry->shard].slab_set[entry->slab]);
    req->pctx.desc = pagechunk_get_desc(req->pctx.slab,entry->slot_idx);
    struct chunk_desc* desc = req->pctx.desc;

    assert(desc!=NULL);
    
    bool slab_changed = slab_is_slab_changed(desc->slab->slab_size,item_packed_size(req->item));
    bool is_cross_page = pagechunk_is_cross_page(desc,entry->slot_idx);

    entry->putting = 1;
    if(slab_changed){
        //The updated item is not situable for the slab, I have to write it
        //into other slab. So I needn't load it and I do not pay 
        //attention to page chunk evicting. Just add the item into other place.
        _process_put_outplace(req,true);
    }
    else if(is_cross_page){
        //The item is stored into multi page, so I needn't
        //load it and I do not pay attention to page chunk evicting.
        //Just add the item into other place.
        _process_put_outplace(req,false);
    }
    else{
        _process_put_inplace(req);
    }
}
