#include "worker_internal.h"
#include "assert.h"
#include "kverrno.h"

#include "spdk/log.h"

static void _default_reclaim_io_cb_fn(void*ctx, int kverrno){
    struct slab_migrate_request *req = ctx;
    if(kverrno){
        req->is_fault = true;
        req->nb_faults++;
    }
    req->nb_processed++;
    req->wctx->rmgr->nb_pending_slots--;
}

/*------------------------------------------------------------------------*/
//Process item migrating.

static void _process_one_pending_migrate_write_tombstone_cb(void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->wctx;

    if(kverrno){
        //Error hits when write tombstone to disk. However, it's OK since 
        //the old one with smaller timestamp will be dropped in recovery.
        //Do nothing.
        SPDK_WARNLOG("Failed to write tombstone, shard:%u,slab:%u,slot:%lu\n",
                      mig->shard_idx,mig->slab->slab_size,mig->slot_idx);
    }

    mig->io_cb_fn(mig->ctx, -KV_ESUCCESS);

    mig->entry->putting = 0;
    mig->entry->slot_idx = mig->new_slot_idx;
    pagechunk_mem_lower(wctx->pmgr,mig->desc);
    pagechunk_mem_lower(wctx->pmgr,mig->new_desc);
    pool_release(wctx->rmgr->pending_migrate_pool,mig);
    //SPDK_NOTICELOG("migrate one slot:%lu, new_slot:%lu\n", mig->slot_idx,mig->new_slot);
}

static void
_process_one_pending_migrate_new_store_data_cb(void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->wctx;
    struct slab* slab = mig->slab;
    
    if(kverrno){
        //Error hits when store data into disk
        mig->io_cb_fn(mig->ctx,-KV_EIO);

        mig->entry->putting = 0;
        slab_free_slot(wctx->rmgr,slab,mig->new_slot_idx);
        pagechunk_mem_lower(wctx->pmgr,mig->desc);
        pagechunk_mem_lower(wctx->pmgr,mig->new_desc);
        pool_release(wctx->rmgr->pending_migrate_pool,mig);
    }
    else{
        //Now write tombstone to the previous slot.
        struct kv_item* item = pagechunk_get_item(wctx->pmgr,mig->desc,mig->slot_idx);
        item->meta.ksize = 0;
        _process_one_pending_migrate_write_tombstone_cb(mig,-KV_ESUCCESS);

        //pagechunk_store_item_meta_async(wctx->pmgr,wctx->imgr,mig->desc,mig->slot_idx,
        //                                _process_one_pending_migrate_write_tombstone_cb,mig);
    }
}

static void
_process_one_pending_migrate_new_load_data_cb(void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->wctx;
    struct slab* slab = mig->slab;

    if(kverrno){
        //Error hits when load data from disk
        mig->io_cb_fn(mig->ctx,-KV_EIO);

        mig->entry->putting = 0;
        slab_free_slot(wctx->rmgr,slab,mig->new_slot_idx);
        pagechunk_mem_lower(wctx->pmgr,mig->desc);
        pagechunk_mem_lower(wctx->pmgr,mig->new_desc);
        pool_release(wctx->rmgr->pending_migrate_pool,mig);
        return;
    }

    struct kv_item *origin_item = pagechunk_get_item(wctx->pmgr,mig->desc,mig->new_slot_idx);
    assert(origin_item->meta.ksize);

    //Now I load the item successfully. Next, I will perform writing.
    pagechunk_put_item(wctx->pmgr,mig->new_desc,mig->new_slot_idx,origin_item);
    pagechunk_store_item_async( wctx->pmgr,
                                wctx->imgr,
                                mig->new_desc,
                                mig->new_slot_idx,
                                _process_one_pending_migrate_new_store_data_cb, 
                                mig);
}

static void 
_process_one_pending_migrate_request_slot_cb(uint64_t slot_idx, void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->wctx;
    struct slab* slab = mig->slab;

    //It is impossible to fail to allocate a slot, since the slab is reclaming. If
    //it happens, the program has bugs.
    assert(!kverrno);
    
    struct chunk_desc* new_desc = pagechunk_get_desc(slab,slot_idx);
    assert(new_desc!=NULL);

    mig->new_slot_idx = slot_idx;
    mig->new_desc = new_desc;

    pagechunk_mem_lift(wctx->pmgr,new_desc);
    pagechunk_load_item_share_async(wctx->pmgr,
                              wctx->imgr, 
                              mig->new_desc,
                              slot_idx,
                              _process_one_pending_migrate_new_load_data_cb,
                              mig);
}

static void
_process_one_pending_migrate_load_data_cb(void* ctx, int kverrno){
    struct pending_item_migrate *mig = ctx;
    struct worker_context *wctx = mig->wctx;
    struct chunk_desc *desc = mig->desc;

    if(kverrno){
        //Error hits when load data from disk
        SPDK_ERRLOG("Failed to load slot data, shard:%u,slab:%u,slot:%lu\n",
                     mig->shard_idx,mig->slab->slab_size,mig->slot_idx);

        mig->io_cb_fn(mig->ctx,-KV_EIO);

        pagechunk_mem_lower(wctx->pmgr,desc);
        pool_release(wctx->rmgr->pending_migrate_pool,mig);
        return;
    }

    //for debug
    mig->io_cb_fn(mig->ctx, -KV_ESUCCESS);
    pagechunk_mem_lower(wctx->pmgr,desc);
    pool_release(wctx->rmgr->pending_migrate_pool,mig);
    return;


    if(!slab_is_slot_occupied(mig->slab,mig->slot_idx)){
        //In the case where I lookup the item and it is in delete or outplace write state. 
        //In the next cycle, I process it again, and it is processed by puting. So
        //When I check the entry again, the slot index is changed.
        //If it is written not-in-place, I needn't do anything.
        mig->io_cb_fn(mig->ctx,-KV_ESUCCESS);
        
        pagechunk_mem_lower(wctx->pmgr,mig->desc);
        pool_release(wctx->rmgr->pending_migrate_pool,mig);
    }

    //Now I load the data into the page chunk cache.
    struct kv_item *item = pagechunk_get_item(wctx->pmgr,desc,mig->slot_idx);  
    assert(item->meta.ksize!=0);

    uint64_t sid = mem_index_lookup(wctx->global_index,item);
    assert(sid);
    assert(mtable_check_valid(wctx->mtable,sid));

    mig->entry = mtable_get(wctx->mtable,sid);
    assert(mig->entry);
    mig->sid = sid;
    
    mig->entry->putting = 1;
    slab_request_slot_async(wctx->imgr,mig->slab,
                            _process_one_pending_migrate_request_slot_cb,
                            mig);
}

static void
_process_one_pending_migrate(struct pending_item_migrate *mig){
    struct worker_context *wctx = mig->wctx;
    struct slab* slab = mig->slab;
    uint64_t slot_idx = mig->slot_idx;

    struct chunk_desc *desc = pagechunk_get_desc(slab,slot_idx);
    assert(desc!=NULL);

    mig->desc = desc;
    pagechunk_mem_lift(wctx->pmgr,desc);
    pagechunk_load_item_async(wctx->pmgr,
                              wctx->imgr, 
                              desc,
                              slot_idx,
                              _process_one_pending_migrate_load_data_cb,
                              mig);
}

//Migrating should not ultilize many IO resources, so they are processed
//by one batch for each cycle.
int 
worker_reclaim_process_pending_item_migrate(struct worker_context *wctx){
    int events = 0;

    uint32_t migrate_batch = wctx->rmgr->migrating_batch;
    uint32_t p_reqs = wctx->rmgr->pending_migrate_pool->count
                      - wctx->rmgr->pending_migrate_pool->nb_frees;

    uint32_t nb_pendings = wctx->rmgr->nb_pending_slots;
    //uint32_t nb = p_reqs > available_slots ? available_slots : p_reqs;

    if(nb_pendings || !p_reqs){
        //no thing to do
        return 0;
    }

    uint32_t nb = p_reqs > migrate_batch ? migrate_batch : p_reqs;
    wctx->rmgr->nb_pending_slots = nb;
    //SPDK_NOTICELOG("pending %u\n migration slots\n", nb);

    struct pending_item_migrate *mig, *tmp = NULL;
    //Put all elements in resubmit queue into pending-migrate queue
    TAILQ_FOREACH_SAFE(mig,&wctx->rmgr->remigrating_head,link,tmp){
        TAILQ_REMOVE(&wctx->rmgr->remigrating_head,mig,link);
        TAILQ_INSERT_HEAD(&wctx->rmgr->item_migrate_head,mig,link);
    }

    TAILQ_FOREACH_SAFE(mig,&wctx->rmgr->item_migrate_head,link,tmp){
        if(!nb){
            break;
        }
        TAILQ_REMOVE(&wctx->rmgr->item_migrate_head,mig,link);
        //SPDK_NOTICELOG("submit a slot request:%lu, %d\n", mig->slot_idx,i++);
        _process_one_pending_migrate(mig);
        nb--;
        events++;
    }
    return events;
}

/*------------------------------------------------------------------------*/
//Process slab reclaim node.

static void
slab_release_complete(void*ctx,int kverrno){
    struct slab_migrate_request *req = ctx;
    struct worker_context* wctx = req->wctx;
    if(kverrno){
        //Does there exist such case ??
        //I do not know what to do.
        assert(0);
    }

    //SPDK_NOTICELOG("Migrating slab completes. shard:%u, slab:%u, node:%u\n",req->shard_id,req->slab_idx,req->node->id);

    struct slab *slab = req->slab;
    struct reclaim_node* node = req->node;
    uint32_t nb_chunks = req->slab->reclaim.nb_chunks_per_node;
    
    uint32_t i=0;
    for(;i<nb_chunks;i++){
        struct chunk_desc *desc = node->desc_array[i];
        assert(desc!=NULL);
        assert(!desc->nb_pendings);
        assert(!desc->dma_buf);

        //bitmap_clear_bit_all(node->desc_array[i]->bitmap);
    }

    //now clear all slots for this node

    //@TODO add hole nodes for a slab. The hole nodes will be firstly allocated of 
    // the slab is requested to expand.

    //Update slab statistics
    //uint32_t nb_slots_per_node = slab->reclaim.nb_chunks_per_node*slab->reclaim.nb_slots_per_chunk;

    //slots_used = nb_slots_per_node - node->nb_free_slots;
    //nb_free_slots -= slot_used + node->nb_free_slots => nb_free_slots -= nb_total_slots;
    //slab->reclaim.nb_free_slots -= nb_slots_per_node;
    //slab->reclaim.nb_total_slots -= nb_slots_per_node;

    //for defragmentation simulation.
    slab->flag &=~ SLAB_FLAG_RECLAIMING;
    //slab_reclaim_free_node(&slab->reclaim,node);

    //Now I finish the request, just release it.
    TAILQ_REMOVE(&wctx->rmgr->slab_migrate_head,req,link);
    pool_release(wctx->rmgr->migrate_slab_pool,req);

    //SPDK_NOTICELOG("Migrating for slab:%u, node:%u, processed:%lu, valid slots:%lu\n",req->slab->slab_size, node->id,req->nb_processed,req->nb_valid_slots);
}

int 
worker_reclaim_process_pending_slab_migrate(struct worker_context *wctx){
    int events = 0;
    struct slab_migrate_request *req = TAILQ_FIRST(&wctx->rmgr->slab_migrate_head);
    if(!req){
        //No pending slab migrating shall be processed.
        return events;
    }

    if(req->is_fault){
        if(req->cur_slot == req->start_slot + req->nb_processed){
            //Faults happened and all the submited item migrating requests have been processed. 
            //So I should abort the migrating.
            SPDK_ERRLOG("Error in migrating, slab:%u, processed:%lu,faults:%u. Abort.\n",
                        req->slab->slab_size,req->nb_processed,req->nb_faults);
            TAILQ_REMOVE(&wctx->rmgr->slab_migrate_head,req,link);
            pool_release(wctx->rmgr->migrate_slab_pool,req);
        }
    }
    else if( req->nb_processed == req->last_slot - req->start_slot + 1 ){
        //all valid slots have been migrated.
        slab_release_node_async(wctx->imgr,req->slab,req->node,slab_release_complete,req);
    }
    else if( req->cur_slot<=req->last_slot ){
        //The slab migrating has not been finished. So, I should process the next batch.
        uint32_t cnt = req->last_slot - req->cur_slot + 1;
        uint32_t a_mig = wctx->rmgr->pending_migrate_pool->nb_frees;
        cnt = cnt < a_mig ? cnt : a_mig;

        while(cnt && req->cur_slot<=req->last_slot){
            if(slab_is_slot_occupied(req->slab,req->cur_slot)){
                //The slot should be migrated.
                struct pending_item_migrate *mig = pool_get(wctx->rmgr->pending_migrate_pool);
                assert(mig!=NULL);
                mig->wctx = wctx;
                mig->slab = req->slab;
                mig->shard_idx = req->shard_id;
                mig->slab_idx  = req->slab_idx;
                mig->slot_idx = req->cur_slot;

                mig->io_cb_fn = _default_reclaim_io_cb_fn;
                mig->ctx = req;
                TAILQ_INSERT_TAIL(&wctx->rmgr->item_migrate_head,mig,link);
            }
            else{
                //This slot is a free slot. I just do nothing.
                req->nb_processed++;
            }
            req->cur_slot++;
            cnt--;
            events++;
        }
    }
    return events;
}
