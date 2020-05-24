#include "worker_internal.h"
#include "assert.h"
#include "kverrno.h"

static void _default_reclaim_io_cb_fn(void*ctx, int kverrno){
    struct slab_migrate_request *req = ctx;
    if(!kverrno){
        req->is_fault = true;
    }
    req->nb_processed++;
}

/*------------------------------------------------------------------------*/
//Process backgound item deleting.

static void
_process_one_pending_delete_store_data_cb(void* ctx, int kverrno){
    struct pending_item_delete *del = ctx;
    struct worker_context *wctx = del->rctx.wctx;
    struct chunk_desc *desc         = del->rctx.desc;

    if(!del->io_cb_fn){
        del->io_cb_fn(del->ctx, (!kverrno)?-KV_EIO:-KV_ESUCCESS);
    }

    pool_release(wctx->rmgr->pending_delete_pool,del);
    desc->flag &=~ CHUNK_PIN;
}

static void
_process_one_pending_delete_load_data_cb(void* ctx, int kverrno){
    struct pending_item_delete *del = ctx;
    struct worker_context *wctx     = del->rctx.wctx;
    struct chunk_desc *desc         = del->rctx.desc;

    if(kverrno){
        //Error hits when load data from disk
        if(!del->io_cb_fn){
            del->io_cb_fn(del->ctx,-KV_EIO);
        }
        pool_release(wctx->rmgr->pending_delete_pool,del);
        desc->flag &=~ CHUNK_PIN;
        return;
    }

    //Now I get only the meta data into the page chunk cache.
    struct kv_item *item = pagechunk_get_item(wctx->pmgr, desc,del->slot_idx);
    item->meta.ksize=0;
    pagechunk_store_item_meta_async(wctx->pmgr,wctx->imgr,desc,del->slot_idx,
                               _process_one_pending_delete_store_data_cb,del);
}

static void 
_process_one_pending_delete_pagechunk_cb(void* ctx, int kverrno){
    struct pending_item_delete *del = ctx;
    struct worker_context *wctx = del->rctx.wctx;
    // We do not care the kverrno, because it must success.
    // Now, load the data from disk.
    assert(!kverrno);

    pagechunk_load_item_meta_async(wctx->pmgr,
                              wctx->imgr, 
                              del->rctx.desc,
                              del->slot_idx,
                              _process_one_pending_delete_load_data_cb,
                              del);
}

static void
_process_one_pending_delete(struct pending_item_delete *del){
    struct worker_context *wctx = del->rctx.wctx;
    struct slab* slab = del->slab;
    uint64_t slot_idx = del->slot_idx;
    struct chunk_desc* desc = pagechunk_get_desc(slab,slot_idx);

    assert(desc!=NULL);
    desc->flag |= CHUNK_PIN;

    del->rctx.desc = desc;

    if(!desc->chunk_mem){
        pagechunk_request_one_async(wctx->pmgr,desc,_process_one_pending_delete_pagechunk_cb,del);
    }
    else if(pagechunk_is_cached(desc,del->slot_idx)){
        _process_one_pending_delete_load_data_cb(del,-KV_ESUCCESS);
    }
    else{
        pagechunk_load_item_meta_async(wctx->pmgr,
                                  wctx->imgr,
                                  desc,
                                  del->slot_idx,
                                  _process_one_pending_delete_load_data_cb,
                                  del);
    }
}

// Each deleting issues a writing io. Just process them as many as possible.
int 
worker_reclaim_process_pending_item_delete(struct worker_context *wctx){
    int events = 0;
    uint32_t a_ios = (wctx->imgr->max_pending_io - wctx->imgr->nb_pending_io);

    struct pending_item_delete *pending_del, *tmp = NULL;
    TAILQ_FOREACH_SAFE(pending_del,&wctx->rmgr->item_delete_head,link,tmp){
        if(a_ios==0){
            break;
        }
        TAILQ_REMOVE(&wctx->rmgr->item_delete_head,pending_del,link);
        pending_del->rctx.wctx = wctx;
        _process_one_pending_delete(pending_del);
        a_ios--;
        events++;
    }
    return events;
}

/*------------------------------------------------------------------------*/
//Process item migrating.

static void
_process_one_pending_migrate_new_store_data_cb(void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->rctx.wctx;
    struct slab* slab = mig->slab;
    
    if(kverrno){
        //Error hits when store data into disk
        mig->io_cb_fn(mig->ctx,-KV_EIO);
        //I have to free the allocated slot
        slab_free_slot_async(wctx->rmgr,slab,mig->new_slot,NULL,NULL);
    }
    else{
        //Wonderful! Now evrything is ok!
        mig->entry->chunk_desc = mig->new_desc;
        mig->entry->slot_idx = mig->new_slot;
        mig->entry->writing = 0;
        slab_free_slot_async(wctx->rmgr,slab,mig->slot_idx,NULL,NULL);
    }
    
    pool_release(wctx->kv_request_internal_pool,mig);
    mig->rctx.desc->flag &=~ CHUNK_PIN;
    mig->new_desc->flag &=~ CHUNK_PIN;
}

static void
_process_one_pending_migrate_new_load_data_cb(void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->rctx.wctx;
    struct slab* slab = mig->slab;

    if(kverrno){
        //Error hits when load data from disk
        mig->io_cb_fn(mig->ctx,-KV_EIO);

        pool_release(wctx->rmgr->pending_migrate_pool,mig);
        mig->rctx.desc->flag &=~ CHUNK_PIN;
        mig->new_desc->flag &=~ CHUNK_PIN;
        //I have to free the allocated slot
        slab_free_slot_async(wctx->rmgr,slab,mig->new_slot,NULL,NULL);
        return;
    }

    struct kv_item *origin_item = pagechunk_get_item(wctx->pmgr,mig->rctx.desc,mig->slot_idx);

    //Now I load the item successfully. Next, I will perform writing.
    pagechunk_put_item(wctx->pmgr,mig->new_desc,mig->new_slot,origin_item);
    pagechunk_store_item_async( wctx->pmgr,
                                wctx->imgr,
                                mig->new_desc,
                                mig->new_slot,
                                _process_one_pending_migrate_new_store_data_cb, 
                                mig);
}

static void
_process_one_pending_migrate_new_pagechunk_request_cb(void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->rctx.wctx;

    // Now, load just the shared pages of the item from disk.
    assert(!kverrno);

    pagechunk_load_item_share_async(wctx->pmgr,
                              wctx->imgr, 
                              mig->rctx.desc,
                              mig->slot_idx,
                              _process_one_pending_migrate_new_load_data_cb,
                              mig);
}

static void 
_process_one_pending_migrate_request_slot_cb(uint64_t slot_idx, void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->rctx.wctx;
    struct slab* slab = mig->slab;

    //It is impossible to fail to allocate a slot, since the slab is reclaming. If
    //it happens, the program has a bug.
    assert(!kverrno);
    
    struct chunk_desc* new_desc = pagechunk_get_desc(slab,mig->slot_idx);

    assert(new_desc!=NULL);
    new_desc->flag |= CHUNK_PIN;

    mig->new_slot = slot_idx;
    mig->new_desc = new_desc;

    if(!new_desc->chunk_mem){
        //Page chunk is evicted. Now request a new page chunk memory
        pagechunk_request_one_async(wctx->pmgr,new_desc,
                                    _process_one_pending_migrate_new_pagechunk_request_cb,mig);
    }else if(pagechunk_is_cached(new_desc,slot_idx)){
        //I needn't load the item from disk, just write it.
        _process_one_pending_migrate_new_load_data_cb(mig,-KV_ESUCCESS);
    }else{
        //I have to load the pages where the item stays in case I write dirty
        //data for other items that also stay in the same pages.
        pagechunk_load_item_share_async( wctx->pmgr,
                                   wctx->imgr,
                                   new_desc,
                                   slot_idx,
                                   _process_one_pending_migrate_new_load_data_cb, 
                                   mig);
    }
}

static void
_process_one_pending_migrate_cached(struct pending_item_migrate *mig){
    struct worker_context *wctx = mig->rctx.wctx;
    struct slab* slab = mig->slab;

    mig->entry->writing = 1;
    slab_request_slot_async(wctx->imgr,slab,
                            _process_one_pending_migrate_request_slot_cb,
                            mig);
}

static void
_process_one_pending_migrate_load_data_cb(void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->rctx.wctx;
    struct chunk_desc *desc = mig->rctx.desc;

    if(kverrno){
        //Error hits when load data from disk
        mig->io_cb_fn(mig->ctx,-KV_EIO);
        pool_release(wctx->rmgr->pending_migrate_pool,mig);
        desc->flag &=~ CHUNK_PIN;
        return;
    }

    //Now I load the data into the page chunk cache.
    struct kv_item *item = pagechunk_get_item(wctx->pmgr, desc,mig->slot_idx);  
    mig->entry = mem_index_lookup(wctx->mem_index,item);
    if(mig->entry->writing){
        mig->rctx.no_lookup = true;
        TAILQ_INSERT_TAIL(&wctx->rmgr->remigrating_head,mig,link);
        return;
    }
    _process_one_pending_migrate_cached(mig);
}

static void
_process_one_pending_migrate_pagechunk_cb(void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->rctx.wctx;
    // We do not care the kverrno, because it must success.
    // Now, load the data from disk.
    assert(!kverrno);

    pagechunk_load_item_async(wctx->pmgr,
                              wctx->imgr, 
                              mig->rctx.desc,
                              mig->slot_idx,
                              _process_one_pending_migrate_load_data_cb,
                              mig);
}

static void
_process_one_pending_migrate(struct pending_item_migrate *mig){
    struct worker_context *wctx = mig->rctx.wctx;
    struct slab* slab = mig->slab;
    if(mig->rctx.no_lookup){
        //I have lookuped the item, and the entry is valid.
        if(mig->entry->writing){
            mig->rctx.no_lookup = true;
            TAILQ_INSERT_TAIL(&wctx->rmgr->remigrating_head,mig,link);
            return;
        }
        //In the case where I lookup the item and it is in writing state. In the next cycle,
        //I process it again, and it is processed by puting but it is written not-in-place. So
        //When I check the entry, it is not in writing state, but the slot index is changed.
        //And I have to check the slot occupation. 
        //If it is written not-in-place, I needn't do anything.
        if(!slab_is_slot_occupied(slab,mig->slot_idx)){
            mig->io_cb_fn(mig->ctx,-KV_ESUCCESS);
            
            pool_release(wctx->rmgr->pending_migrate_pool,mig);
            mig->rctx.desc->flag &=~ CHUNK_PIN;
            return;
        }
    }

    uint64_t slot_idx = mig->slot_idx;
    struct chunk_desc *desc = pagechunk_get_desc(slab,slot_idx);
    
    assert(desc!=NULL);
    desc->flag |= CHUNK_PIN;

    mig->rctx.desc = desc;
    //The chunk_mem should not be evicted.
    desc->flag |= CHUNK_PIN;

    if(!desc->chunk_mem){
        pagechunk_request_one_async(wctx->pmgr,desc,_process_one_pending_migrate_pagechunk_cb,mig);
    }
    else if(pagechunk_is_cached(desc,mig->slot_idx)){
        _process_one_pending_migrate_load_data_cb(mig,-KV_ESUCCESS);
    }
    else{
        pagechunk_load_item_async(wctx->pmgr,
                                  wctx->imgr,
                                  desc,
                                  slot_idx,
                                  _process_one_pending_migrate_load_data_cb,
                                  mig);
    }
}

//Migrating should not ultilize many IO resources, so they are processed
//by one batch for each cycle.
int 
worker_reclaim_process_pending_item_migrate(struct worker_context *wctx){
    int events = 0;
    uint32_t a_ios = (wctx->imgr->max_pending_io - wctx->imgr->nb_pending_io);
    uint32_t migrate_batch = wctx->rmgr->migrating_batch;
    uint32_t nb = migrate_batch < a_ios ? migrate_batch : a_ios;

    struct pending_item_migrate *mig, *tmp = NULL;

    //Put all elements in reesubmit queue into pending-migrate queue
    TAILQ_FOREACH_SAFE(mig,&wctx->rmgr->remigrating_head,link,tmp){
        TAILQ_REMOVE(&wctx->rmgr->remigrating_head,mig,link);
        TAILQ_INSERT_HEAD(&wctx->rmgr->item_migrate_head,mig,link);
    }

    TAILQ_FOREACH_SAFE(mig,&wctx->rmgr->item_migrate_head,link,tmp){
        if(nb==0){
            break;
        }
        TAILQ_REMOVE(&wctx->rmgr->item_migrate_head,mig,link);
        _process_one_pending_migrate(mig);
        nb--;
        events++;
    }
    return events;
}

/*------------------------------------------------------------------------*/
//Process slab reclaim node.

static void
slab_truncate_complete(void*ctx,int kverrno){
    struct slab_migrate_request *req = ctx;
    struct worker_context* wctx = req->wctx;
    if(kverrno){
        //Error in resie the slab
        //Does there exist such case ??
        //I do not know what to do.
        assert(0);
    }

    struct slab *slab = req->slab;
    struct reclaim_node* node = req->node;

    //Delete all the page chunks.
    uint32_t nb_chunks = req->slab->reclaim.nb_chunks_per_node;
    
    uint32_t i=0;
    for(;i<nb_chunks;i++){
        struct chunk_desc *desc = req->node->desc_array[i];

        assert(desc!=NULL);
        assert(desc->flag|CHUNK_PIN);

        if(desc->chunk_mem){
            //Free it from global lru list
            pagechunk_release_one(wctx->pmgr,desc->chunk_mem);
            TAILQ_REMOVE(&wctx->pmgr->global_chunks,desc,link);
            wctx->pmgr->nb_used_chunks--;
        }
    }
     //Update slab statistics
    uint32_t nb_slots_per_node = slab->reclaim.nb_chunks_per_node*slab->reclaim.nb_slots_per_chunk;

    //slots_used = nb_slots_per_node - node->nb_free_slots;
    //nb_free_slots -= slot_used + node->nb_free_slots => nb_free_slots -= nb_total_slots;
    slab->reclaim.nb_free_slots -= nb_slots_per_node;
    slab->reclaim.nb_total_slots -= nb_slots_per_node;

    slab->flag &=~ SLAB_FLAG_RECLAIMING;
    slab_reclaim_free_node(&slab->reclaim,node);

    //Now I finish the request, just release it.
    TAILQ_REMOVE(&wctx->rmgr->slab_migrate_head,req,link);
    pool_release(wctx->rmgr->migrate_slab_pool,req);
}

int 
worker_reclaim_process_pending_slab_migrate(struct worker_context *wctx){
    int events = 0;
    uint32_t a_mig = wctx->rmgr->pending_migrate_pool->nb_frees;
    struct slab_migrate_request *req = TAILQ_FIRST(&wctx->rmgr->slab_migrate_head);
    if(!req){
        //No pending slab migrating shall be processed.
        return events;
    }

    if(req->is_fault){
        if(req->cur_slot == req->start_slot + req->nb_processed - 1){
            //Faults happened and all the submited item migrating request have been processed. 
            //So I should abort the migrating.
            //Error here
            //Print the error log.
            TAILQ_REMOVE(&wctx->rmgr->slab_migrate_head,req,link);
            pool_release(wctx->rmgr->migrate_slab_pool,req);
        }
    }
    else if( (req->cur_slot==req->last_slot) && (req->nb_processed == req->cur_slot - req->start_slot + 1) ){
        //I have submited all the item-migrating request. And all submited requests have been
        //processed.
        slab_truncate_async(wctx->imgr,req->slab,1,slab_truncate_complete,req);
    }
    else if( req->cur_slot!=req->last_slot ){
        //The slab migrating has not been finished. So, I should process the next batch.
        uint32_t cnt = req->last_slot - req->cur_slot;
        cnt = cnt < a_mig ? cnt : a_mig;
        uint32_t i = 0;
        req->cur_slot += 1;
        while(i<cnt && req->cur_slot<=req->last_slot){
            if(slab_is_slot_occupied(req->slab,req->cur_slot)){
                //The slot should be migrated.
                struct pending_item_migrate *mig = pool_get(wctx->rmgr->pending_migrate_pool);
                assert(mig!=NULL);

                mig->slab = req->slab;
                mig->slot_idx = req->cur_slot;
                mig->io_cb_fn = _default_reclaim_io_cb_fn;
                mig->ctx = req;
                mig->entry = NULL;
                mig->rctx.wctx = wctx;
                mig->rctx.no_lookup = false;
                TAILQ_INSERT_TAIL(&wctx->rmgr->item_migrate_head,mig,link);
                i++;
            }
            //This slot is a free slot. I just do nothing.
            req->cur_slot++;
            req->nb_processed++;
            events++;
        }
    }
    return events;
}

void slab_reclaim_post_delete(struct reclaim_mgr* rmgr,
                          struct slab* slab, 
                          uint64_t slot_idx,
                          void (*cb)(void* ctx, int kverrno),
                          void* ctx){
    
    struct pending_item_delete *pending_del = pool_get(rmgr->pending_delete_pool);
    assert(pending_del!=NULL);

    pending_del->slab = slab;
    pending_del->slot_idx = slot_idx;
    pending_del->io_cb_fn = cb;
    pending_del->ctx = ctx;
    
    TAILQ_INSERT_TAIL(&rmgr->item_delete_head,pending_del,link);
}
