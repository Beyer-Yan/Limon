#include <assert.h>
#include "worker_internal.h"
#include "kverrno.h"

#include "spdk/log.h"

/*----------------------------------------------------------*/
//Here are common functions
//Nothind currently

/*----------------------------------------------------------*/

static void
_process_put_update_not_in_place_item_store_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct index_entry* new_entry   = &pctx->new_entry;
    struct chunk_desc *desc = new_entry->chunk_desc;

    if(kverrno){
        //Store data error, which may be caused by IO error.
        slab_free_slot_async(wctx->rmgr,pctx->slab,new_entry->slot_idx,NULL,NULL);  
    }
    else{
        //Wonderful! Everthing is OK!
        req->cb_fn(req->ctx,NULL,-KV_ESUCCESS);
        entry->chunk_desc = new_entry->chunk_desc;
        entry->slot_idx = new_entry->slot_idx;
        entry->writing = 0;
        //Now I have to reclaim the old slot index. But it is posted to 
        //background stage
        if(!pctx->old_slab){
            //I just release the slot index in the old data in such case.
            slab_free_slot_async(wctx->rmgr,pctx->old_slab,entry->slot_idx,NULL,NULL);
        }
        else{
        //The item is put in the same slab
            slab_free_slot_async(wctx->rmgr,pctx->slab,entry->slot_idx,NULL,NULL);
        }
    }
    pool_release(wctx->kv_request_internal_pool,req);
    desc->flag &=~ CHUNK_PIN;
    entry->writing = 0;
    req->cb_fn(req->ctx,NULL, kverrno ? -KV_EIO : -KV_ESUCCESS);
}

static void
_process_put_update_not_in_place_item_load_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct index_entry* new_entry   = &pctx->new_entry;

    if(kverrno){
        //Load data error, which may be caused by IO error.
        pool_release( wctx->kv_request_internal_pool,req);
        entry->writing = 0;
        entry->chunk_desc->flag &=~ CHUNK_PIN;
        //I have to free the allocated slot
        slab_free_slot_async(wctx->rmgr,pctx->slab,new_entry->slot_idx,NULL,NULL);  
        
        req->cb_fn(req->ctx,NULL,-KV_EIO);
        return;
    }

    //Now I load the item successfully. Next, I will perform writing.
    pagechunk_put_item(wctx->pmgr,new_entry->chunk_desc,new_entry->slot_idx,req->item);
    pagechunk_store_item_async( wctx->pmgr,
                                wctx->imgr,
                                new_entry->chunk_desc,
                                new_entry->slot_idx,
                                _process_put_update_not_in_place_item_store_data_cb, 
                                req);
}

static void _process_put_update_not_in_place_item_pagechunk_request_cb(void* ctx, int kverrno){

    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);
    // Now, load the data from disk for the newly allocated page chunk memory.
    assert(!kverrno);
    pagechunk_load_item_share_async( pctx->wctx->pmgr, 
                               pctx->wctx->imgr, 
                               pctx->new_entry.chunk_desc,
                               pctx->new_entry.slot_idx,
                               _process_put_update_not_in_place_item_load_data_cb,
                               req);

}

static void 
_process_put_update_not_in_place_item_request_slot_cb(uint64_t slot_idx, void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    
    if(kverrno){
        //Slot request failed. This may be caused by out-of-disk-space.
        pool_release(wctx->kv_request_internal_pool,req);
        entry->writing = 0;
        req->cb_fn(req->ctx,NULL,-KV_EFULL);
        return;
    }
    struct chunk_desc* new_desc = pagechunk_get_desc(pctx->slab,slot_idx);

    assert(new_desc!=NULL);
    new_desc->flag |= CHUNK_PIN;
    pctx->new_entry.slot_idx = slot_idx;
    pctx->new_entry.chunk_desc = new_desc;

    if(!new_desc->chunk_mem){
        //Page chunk is evicted. Now request a new page chunk memory
        pagechunk_request_one_async(wctx->pmgr,new_desc,
                                    _process_put_update_not_in_place_item_pagechunk_request_cb,req);
    }else if(pagechunk_is_cached(new_desc,slot_idx)){
        //I needn't load the item from disk, just write it.
        _process_put_update_not_in_place_item_load_data_cb(req,-KV_ESUCCESS);
    }else{
        //I have to load the pages where the item stays in case I write dirty
        //data for other items that also stay in the same pages.
        pagechunk_load_item_share_async( wctx->pmgr,
                                   wctx->imgr,
                                   new_desc,
                                   slot_idx,
                                   _process_put_update_not_in_place_item_load_data_cb, 
                                   req);
    }
}

static void 
_process_put_update_not_in_place_item(struct kv_request_internal *req, bool slab_changed){
    struct process_ctx *pctx = &(req->pctx);
    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;

    pctx->slab = entry->chunk_desc->slab;

    if(slab_changed){
        uint32_t new_slab_idx = slab_find_slab(item_packed_size(req->item));
        uint32_t shard_idx = req->shard;

        struct slab* new_slab = &wctx->shards[shard_idx].slab_set[new_slab_idx];
        pctx->slab = new_slab;
        pctx->old_slab = pctx->slab;
    }

    slab_request_slot_async(wctx->imgr,pctx->slab,
                                 _process_put_update_not_in_place_item_request_slot_cb,
                                 req);
}

/*-------------------------------------------------------------*/

static void
_process_put_add_store_data_cb(void*ctx, int kverrno){
    
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct chunk_desc *desc = entry->chunk_desc;

    if(kverrno){
        //Store data error, which may be caused by IO error.
        //The newly inseerted index shall be deleted.
        slab_free_slot_async(wctx->rmgr,pctx->slab,entry->slot_idx,NULL,NULL);
        mem_index_delete(wctx->mem_index,req->item);    
    }

    //SPDK_NOTICELOG("Put storing completes for key:%d, err:%d\n",*(int*)req->item->data,kverrno);

    pool_release(wctx->kv_request_internal_pool,req);
    desc->flag &=~ CHUNK_PIN;
    entry->writing = 0;

    req->cb_fn(req->ctx,NULL,kverrno ? -KV_EIO : -KV_ESUCCESS);
}

static void
_process_put_add_load_data_cb(void*ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;

    if(kverrno){
        //Load data error, which may be caused by IO error.
        pool_release(wctx->kv_request_internal_pool,req);
        entry->chunk_desc->flag &=~ CHUNK_PIN;
        //I have to free the allocated slot in background
        slab_free_slot_async(wctx->rmgr,pctx->slab,entry->slot_idx,NULL,NULL);
        mem_index_delete(wctx->mem_index,req->item); 

        req->cb_fn(req->ctx,NULL,-KV_EIO); 
        return;  
    }

    //SPDK_NOTICELOG("Put add new load data completes, storing key:%d\n",*(int*)req->item->data);

    //Now I load the item successfully. Next, I will perform writing.
    pagechunk_put_item(wctx->pmgr,entry->chunk_desc,entry->slot_idx,req->item);
    pagechunk_store_item_async( wctx->pmgr,
                                wctx->imgr,
                                entry->chunk_desc,
                                entry->slot_idx,
                                _process_put_add_store_data_cb, 
                                req);
}

static void 
_process_put_add_pagechunk_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &(req->pctx);

    // Now, load the data from disk.
    assert(!kverrno);
    pagechunk_load_item_share_async( pctx->wctx->pmgr,
                               pctx->wctx->imgr, 
                               pctx->entry->chunk_desc,
                               pctx->entry->slot_idx,
                               _process_put_add_load_data_cb,
                               req);
}

static void
_process_put_add_request_slot_cb(uint64_t slot_idx, void* ctx, int kverrno){
    
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
    desc->flag |= CHUNK_PIN;
    entry->chunk_desc = desc;
    entry->slot_idx = slot_idx;

    if(!desc->chunk_mem){
        //Page chunk is evicted. Now request a new page chunk memory
        pagechunk_request_one_async(wctx->pmgr,desc,_process_put_add_pagechunk_cb,req);
    }else if(pagechunk_is_cached(desc,slot_idx)){
        //I needn't load the item from disk, just write it.
        _process_put_add_load_data_cb(req,-KV_ESUCCESS);
    }else{
        //I have to load the pages where the item stays in case I write dirty
        //data for other items that also stay in the same pages.
        pagechunk_load_item_share_async(wctx->pmgr,
                                  wctx->imgr,
                                  desc,
                                  slot_idx,
                                  _process_put_add_load_data_cb, 
                                  req);
    }
}

static void
_process_put_add(struct kv_request_internal *req){

    struct worker_context *wctx = req->pctx.wctx;

    struct index_entry new_entry = {0};
    new_entry.writing = 1;
    new_entry.slot_idx = 0;
    new_entry.chunk_desc = NULL;

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
    req->pctx.entry = entry;
    uint32_t shard_idx = req->shard;
    uint32_t slab_idx = slab_find_slab(item_packed_size(req->item));

    struct slab* slab = &(wctx->shards[shard_idx].slab_set[slab_idx]);
    req->pctx.slab = slab;

    //SPDK_NOTICELOG("Get slab from shard:%u, slab size:%u\n",req->shard,slab->slab_size);

    slab_request_slot_async(wctx->imgr,slab,_process_put_add_request_slot_cb,req);
}

/*--------------------------------------------------------------------*/

static void
_process_put_single_page_store_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct chunk_desc *desc         = entry->chunk_desc;

    pool_release(wctx->kv_request_internal_pool,req);
    entry->writing = 0;
    desc->flag &=~ CHUNK_PIN;

    req->cb_fn(req->ctx, NULL, kverrno ? -KV_EIO : -KV_ESUCCESS);
}

static void
_process_put_update_single_page_cached(struct kv_request_internal *req){

    struct process_ctx *pctx = &req->pctx;

    pagechunk_put_item(pctx->wctx->pmgr,
                       pctx->entry->chunk_desc, 
                       pctx->entry->slot_idx,
                       req->item);

    pagechunk_store_item_async(pctx->wctx->pmgr,
                               pctx->wctx->imgr,
                               pctx->entry->chunk_desc,
                               pctx->entry->slot_idx,
                               _process_put_single_page_store_data_cb,
                               req);
}

static void
_process_put_single_page_load_data_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;

    struct worker_context *wctx     = pctx->wctx;
    struct index_entry* entry       = pctx->entry;
    struct chunk_desc *desc         = entry->chunk_desc;

    if(kverrno){
        //Error hits when load data from disk
        pool_release(wctx->kv_request_internal_pool,req);
        entry->writing = 0;
        desc->flag &=~ CHUNK_PIN;
        
        req->cb_fn(req->ctx,NULL,-KV_EIO);
        return;
    } 

    _process_put_update_single_page_cached(req);
}

static void 
_process_put_single_page_pagechunk_cb(void* ctx, int kverrno){
    struct kv_request_internal *req = ctx;
    struct process_ctx *pctx = &req->pctx;
    // Now, load the data from disk.
    assert(!kverrno);

    pagechunk_load_item_share_async(pctx->wctx->pmgr,
                              pctx->wctx->imgr, 
                              pctx->entry->chunk_desc,
                              pctx->entry->slot_idx,
                              _process_put_single_page_load_data_cb,
                              req);
}

void worker_process_put(struct kv_request_internal *req){
    struct index_entry *entry = req->pctx.entry;
    struct worker_context *wctx = req->pctx.wctx;

    if(!entry){
        //item does not exist. It a new item!
        //SPDK_NOTICELOG("put new item, item->key:%d\n",*(int*)req->item->data);
        _process_put_add(req);
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
    struct chunk_desc *desc = entry->chunk_desc;
    entry->writing = 1;

    assert(desc!=NULL);
    desc->flag |= CHUNK_PIN;

    SPDK_NOTICELOG("update item, item->key:%d\n",*(int*)req->item->data);

    if(pagechunk_is_cross_page(desc,entry->slot_idx)){
        //The item is stored into multi page, so I needn't
        //load it and I do not pay attention to page chunk evicting.
        //Just add the item into other place.
        _process_put_update_not_in_place_item(req,false);
    }
    else if(slab_is_slab_changed(desc->slab_size,item_packed_size(req->item))){
        //The updated item is not situable for the slab, I have to write it
        //into other slab. So I needn't load it and I do not pay 
        //attention to page chunk evicting. Just add the item into other place.
        _process_put_update_not_in_place_item(req,true);
    }
    else{
        // Now I know that the item is stored into single page. So if the page chunk
        // is evicted, I have to get a new page chunk memory.
        if(!desc->chunk_mem){
            //Page chunk is evicted , Now request a new page chunk memory
            pagechunk_request_one_async(wctx->pmgr,desc,_process_put_single_page_pagechunk_cb,req);
        }
        else if(pagechunk_is_cached(desc,entry->slot_idx)){
            //The item is cached and stored in single page, just update in place
            _process_put_update_single_page_cached(req);
        }
        else{
            //The item is stored into a single page but not cached, so I have to load 
            //the data into page chunk cache before I perform update
            pagechunk_load_item_share_async(wctx->pmgr,wctx->imgr, desc,entry->slot_idx,
                                      _process_put_single_page_load_data_cb,req);
        }
    }
}
