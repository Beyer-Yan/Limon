
#include "worker_internal.h"
#include "kverrno.h"
#include "reclaim.h"
#include "slab.h"
#include "rbtree_uint.h"
#include "kvutil.h"

#define DEFAULT_RECLAIM_POLLING_PERIOD_MS 10000

static void 
_process_one_kv_request(struct worker_context *wctx, struct kv_request_internal *req){
    if(!req->pctx.no_lookup){
        req->pctx.entry  = mem_index_lookup(wctx->mem_index,req->item);
    }
    req->pctx.wctx = wctx;
    req->pctx.slab = NULL;
    req->pctx.old_slab = NULL;

    switch(req->op_code){
        case GET:
            worker_process_get(req);
            break;
        case PUT:
            worker_process_put(req);
            break;
        case DELETE:
            worker_process_delete(req);
            break;
        case FIRST:
            worker_process_first(req);
            break;
        case SEEK:
            worker_process_next(req);
            break;
        case NEXT:
            worker_process_next(req);
            break;
        default:
            req->cb_fn(req->ctx,NULL,-KV_EOP_UNKNOWN);
            pool_release(&wctx->kv_request_internal_pool,req);
            break;
    }
}

static void
_worker_business_processor_poll(struct worker_context *wctx){

    // The pending requests in the worker context no-lock request queue
    uint32_t p_reqs = wctx->sent_requests - wctx->processed_requests;

    // The avalaible pending requests in the worker request pool
    uint32_t a_reqs = wctx->kv_request_internal_pool->nb_frees;

    //The avalaible disk io.
    uint32_t a_ios = (wctx->imgr->max_pending_io - wctx->imgr->nb_pending_io);

    /**
     * @brief Get the minimum value from the rquests queue, kv request internal pool
     * and disk io manager
     */
    uint32_t process_size = p_reqs < a_reqs ? p_reqs : a_reqs;
    process_size = process_size < a_ios ? process_size : a_ios;

    uint32_t i = 0;
    struct kv_request_internal *req_internal,*tmp=NULL;
    struct kv_request *req;

    /**
     * @brief Process the resubmit queue. I needn't get new cached requests
     * from requests pool, since requests from resubmit queue already have 
     * cached request object.
     * Just link then to submit queue.
     */
    TAILQ_FOREACH_SAFE(req_internal,&wctx->resubmit_queue,link,tmp){
            TAILQ_REMOVE(&wctx->resubmit_queue,req_internal,link);
            TAILQ_INSERT_HEAD(&wctx->submit_queue,req_internal,link);
    }

    /**
     * @brief Process the submit queue. For every kv request from users, I have to
     * get a request_internal object from request object pool to wrap it.
     */
    if(process_size > 0){
        for(;i<process_size;i++){
            req = &wctx->request_queue[wctx->processed_requests%wctx->max_pending_kv_request];
            req_internal = pool_get(wctx->kv_request_internal_pool);
            assert(req_internal!=NULL);

            req_internal->ctx = req->ctx;
            req_internal->item = req->item;
            req_internal->op_code = req->op_code;
            req_internal->cb_fn = req->cb_fn;
            req_internal->shard = req->shard;

            //I have to perform a lookup, since it is a new request.
            req_internal->pctx.no_lookup = false;

            TAILQ_INSERT_TAIL(&wctx->submit_queue,req_internal,link);
            wctx->processed_requests++;
        }
    }
    TAILQ_FOREACH_SAFE(req_internal, &wctx->submit_queue,link,tmp){
        TAILQ_REMOVE(&wctx->submit_queue,req_internal,link);
        _process_one_kv_request(wctx, req_internal);
    }
}

static void
_worker_reclaim_poll(struct worker_context *wctx){
    worker_reclaim_process_pending_slab_migrate(wctx);
    worker_reclaim_process_pending_item_migrate(wctx);
    worker_reclaim_process_pending_item_delete(wctx);
}

static void
_fill_slab_migrate_req(struct slab_migrate_request *req, struct slab* slab ){
    //Submit a slab shrinking request
    req->slab = slab;
    req->is_fault = false;
    req->node = rbtree_last(slab->reclaim.total_tree);

    req->nb_processed = 0;
    req->start_slot = reclaim_get_start_slot(&slab->reclaim,req->node);
    req->cur_slot = req->start_slot;
    req->last_slot = slab->reclaim.nb_total_slots-1;

    if(!rbtree_lookup(slab->reclaim.free_node_tree,slab->reclaim.nb_reclaim_nodes-1)){
        //The last reclaim node is not allowed to performing allocating.  
        rbtree_delete(slab->reclaim.free_node_tree,slab->reclaim.nb_reclaim_nodes-1,NULL);
    }
}
//This poller shall be excecuted regularly, e.g. once a second.
static void
_worker_slab_evaluation_poll(struct worker_context *wctx){
    uint32_t i=0,j=0;
    for(;i < wctx->nb_reclaim_shards;i++){
        struct slab_shard *shard = &wctx->shards[wctx->reclaim_shards_start_id + i];
        for(;j < shard->nb_slabs;j++){
            if( !(shard->slab_set[j].flag | SLAB_FLAG_RECLAIMING) ){
                struct slab* slab = &(shard->slab_set[j]);
                if( reclaim_evaluate_slab(slab) ){
                    struct slab_migrate_request *slab_migrate_req = pool_get(wctx->rmgr->migrate_slab_pool);
                    assert(slab_migrate_req!=NULL);

                    _fill_slab_migrate_req(slab_migrate_req,slab);
                    slab->flag |= SLAB_FLAG_RECLAIMING;
                    TAILQ_INSERT_TAIL(&wctx->rmgr->slab_migrate_head,slab_migrate_req,link);
                }
            }
        }
    }
}

// Get a slot buffer from woker request queue.
static uint32_t
get_request_buffer(struct worker_context *wctx) {
    uint32_t next_buffer = __sync_fetch_and_add(&wctx->buffered_request_idx, 1);
    while(1) {
        volatile uint32_t pending = next_buffer - wctx->processed_requests;
        if(pending >= wctx->max_pending_kv_request) { // Queue is full, wait
            NOP10();
        } else {
            break;
        }
    }
    return next_buffer % wctx->max_pending_kv_request;
}

// Once we get a slot, we fill it, and then submit it */
static uint32_t
submit_request_buffer(struct worker_context *wctx, uint32_t buffer_idx) {
    while(1) {
        if(wctx->sent_requests%wctx->max_pending_kv_request != buffer_idx) { 
        // Somebody else is enqueuing a request, wait!
            NOP10();
        } else {
            break;
        }
    }
    return __sync_fetch_and_add(&wctx->sent_requests, 1);
}

static inline void
_worker_enqueue_common(struct kv_request* req, uint32_t shard,const struct kv_item *item, 
                       kv_cb cb_fn,void* ctx,
                       enum op_code op){
    req->cb_fn = cb_fn;
    req->ctx = ctx;
    req->item = item;
    req->op_code = GET;
    req->shard = shard;
}

void worker_enqueue_get(struct worker_context* wctx,uint32_t shard,const struct kv_item *item, 
                        kv_cb cb_fn, void* ctx){
    unsigned int buffer_slot  = get_request_buffer(wctx);
    struct kv_request *req = &wctx->request_queue[buffer_slot];
    _worker_enqueue_common(req,shard,item,cb_fn,ctx,GET);
    submit_request_buffer(wctx,buffer_slot);
}

void worker_enqueue_put(struct worker_context* wctx,uint32_t shard,const struct kv_item *item,
                        kv_cb cb_fn, void* ctx){
    unsigned int buffer_slot  = get_request_buffer(wctx);
    struct kv_request *req = &wctx->request_queue[buffer_slot];
    _worker_enqueue_common(req,shard,item,cb_fn,ctx,PUT);
    submit_request_buffer(wctx,buffer_slot);
}

void worker_enqueue_delete(struct worker_context* wctx,uint32_t shard,const struct kv_item *item, 
                           kv_cb cb_fn, void* ctx){
    unsigned int buffer_slot  = get_request_buffer(wctx);
    struct kv_request *req = &wctx->request_queue[buffer_slot];
    _worker_enqueue_common(req,shard,item,cb_fn,ctx,DELETE);
    submit_request_buffer(wctx,buffer_slot);
}

void worker_enqueue_first(struct worker_context* wctx,uint32_t shard,const struct kv_item *item,
                         kv_cb cb_fn, void* ctx){
    unsigned int buffer_slot  = get_request_buffer(wctx);
    struct kv_request *req = &wctx->request_queue[buffer_slot];
    _worker_enqueue_common(req,shard,item,cb_fn,ctx,FIRST);
    submit_request_buffer(wctx,buffer_slot);
}

void worker_enqueue_seek(struct worker_context* wctx,uint32_t shard,const struct kv_item *item,
                         kv_cb cb_fn, void* ctx){
    unsigned int buffer_slot  = get_request_buffer(wctx);
    struct kv_request *req = &wctx->request_queue[buffer_slot];
    _worker_enqueue_common(req,shard,item,cb_fn,ctx,SEEK);
    submit_request_buffer(wctx,buffer_slot);
}

void worker_enqueue_next(struct worker_context* wctx,uint32_t shard,const struct kv_item *item, 
                         kv_cb cb_fn, void* ctx){
    unsigned int buffer_slot  = get_request_buffer(wctx);
    struct kv_request *req = &wctx->request_queue[buffer_slot];
    _worker_enqueue_common(req,shard,item,cb_fn,ctx,NEXT);
    submit_request_buffer(wctx,buffer_slot);
}

struct worker_context* 
worker_init(struct worker_init_opts* opts){
    uint32_t req_queue_size;
    uint32_t req_pool_size;
    uint32_t pmgr_size;
    uint32_t rmgr_size;
    uint32_t imgr_size;

    uint32_t req_pool_header_size;
    uint32_t req_pool_data_size;

    uint32_t pmgr_req_pool_header_size;
    uint32_t pmgr_ctx_pool_header_size;
    uint32_t pmgr_req_pool_data_size;
    uint32_t pmgr_ctx_pool_data_size;

    uint32_t rmgr_del_pool_header_size;
    uint32_t rmgr_mig_pool_header_size;
    uint32_t rmgr_slab_pool_header_size;
    uint32_t rmgr_del_pool_data_size;
    uint32_t rmgr_mig_pool_data_size;
    uint32_t rmgr_slab_pool_data_size;

    uint32_t imgr_cache_pool_header_size;
    uint32_t imgr_page_pool_header_size;
    uint32_t imgr_cache_pool_data_size;
    uint32_t imgr_page_pool_data_size;

    uint32_t nb_max_reqs = opts->max_request_queue_size_per_worker;
    uint32_t nb_max_ios  = opts->max_io_pending_queue_size_per_worker;
    uint32_t nb_slabs_per_shard = opts->nb_reclaim_shards*opts->shard[0].nb_slabs;

    req_queue_size = nb_max_reqs * sizeof(struct kv_request);

    req_pool_header_size = pool_header_size(nb_max_reqs);
    req_pool_data_size = nb_max_reqs*sizeof(struct kv_request_internal);
    req_pool_size = req_pool_header_size + req_pool_data_size;
                    
    
    pmgr_req_pool_header_size = pool_header_size(nb_max_ios);
    pmgr_ctx_pool_header_size = pool_header_size(nb_max_ios);
    pmgr_req_pool_data_size   = nb_max_ios*sizeof(struct chunk_miss_callback);
    pmgr_ctx_pool_data_size   = nb_max_ios*sizeof(struct chunk_load_store_ctx);

    rmgr_del_pool_header_size = pool_header_size(nb_max_ios);
    rmgr_mig_pool_header_size = pool_header_size(nb_max_ios);
    rmgr_slab_pool_header_size = pool_header_size(nb_slabs_per_shard);
    rmgr_del_pool_data_size  = nb_max_ios*sizeof(struct pending_item_delete);
    rmgr_mig_pool_data_size  = nb_max_ios*sizeof(struct pending_item_migrate);
    rmgr_slab_pool_data_size = nb_slabs_per_shard*sizeof(struct slab_migrate_request);
   
    imgr_cache_pool_header_size = pool_header_size(nb_max_ios);
    imgr_page_pool_header_size = pool_header_size(nb_max_ios*2);
    imgr_cache_pool_data_size  = nb_max_ios*sizeof(struct cache_io);
    imgr_page_pool_data_size   = nb_max_ios*2*sizeof(struct page_io);

    pmgr_size = sizeof(struct pagechunk_mgr) + pmgr_req_pool_header_size + pmgr_ctx_pool_header_size +
                pmgr_req_pool_data_size + pmgr_ctx_pool_data_size;
    
    rmgr_size = sizeof(struct reclaim_mgr) + rmgr_del_pool_header_size + rmgr_mig_pool_header_size +
                rmgr_slab_pool_header_size + rmgr_del_pool_data_size + rmgr_mig_pool_data_size +
                rmgr_slab_pool_data_size;
    
    imgr_size = sizeof(struct iomgr) + imgr_cache_pool_header_size + imgr_page_pool_header_size + 
                imgr_cache_pool_data_size + imgr_page_pool_data_size;
    
    uint32_t total_size = sizeof(struct worker_context) + req_queue_size + req_pool_size +
                          pmgr_size + rmgr_size + imgr_size;
    
    struct worker_context *wctx = malloc(total_size);
    assert(wctx!=NULL);

    struct spdk_thread* thread;

    wctx->mem_index = mem_index_init();
    wctx->shards = opts->nb_shards;
    wctx->nb_shards = opts->nb_shards;
    wctx->nb_reclaim_shards = opts->nb_reclaim_shards;
    wctx->reclaim_shards_start_id = opts->reclaim_shard_start_id;
    wctx->reclaim_percentage_threshold = opts->reclaim_percentage_threshold;
    wctx->request_queue = (struct kv_req*)(wctx + 1);
    wctx->buffered_request_idx = 0;
    wctx->sent_requests = 0;
    wctx->processed_requests = 0;
    wctx->max_pending_kv_request = opts->max_request_queue_size_per_worker;
    TAILQ_INIT(&wctx->submit_queue);
    TAILQ_INIT(&wctx->resubmit_queue);
    wctx->kv_request_internal_pool = (struct object_cache_pool*)(wctx->request_queue + wctx->max_pending_kv_request);
    pool_header_init(wctx->kv_request_internal_pool,nb_max_reqs,sizeof(struct kv_request),req_pool_header_size,
                     wctx->kv_request_internal_pool+1);
    
    struct spdk_cpuset cpumask;
    //The name will be copied. So the stack variable makes sense.
    char thread_name[32];

    spdk_cpuset_zero(&cpumask);
    spdk_cpuset_get_cpu(&cpumask,opts->core_id);
    snprintf(thread_name,sizeof(thread_name),"worker_%u",opts->core_id);
    wctx->target = opts->target;
    wctx->thread = spdk_thread_create(thread_name,&cpumask);
    assert(wctx->thread!=NULL);

    struct pagechunk_mgr *pmgr = (uint8_t*)(wctx+1) + req_queue_size + req_pool_size;
    struct reclaim_mgr   *rmgr = (uint8_t*)pmgr + pmgr_size;
    struct iomgr         *imgr = (uint8_t*)rmgr + rmgr_size;

    //Page chunk maneger runtime init
    TAILQ_INIT(&pmgr->global_chunks);
    pmgr->hit_times = 0;
    pmgr->miss_times = 0;
    pmgr->chunkmgr_worker = opts->chunkmgr_worker;
    
    pmgr->kv_chunk_request_pool = (struct object_cache_pool*)(pmgr+1);
    pool_header_init(pmgr->kv_chunk_request_pool,nb_max_ios,sizeof(struct chunk_miss_callback),pmgr_req_pool_header_size,
                     pmgr->kv_chunk_request_pool+1);
    
    pmgr->load_store_ctx_pool = (uint8_t*)pmgr->kv_chunk_request_pool + pmgr_req_pool_header_size + 
                                pmgr_req_pool_data_size;
    pool_header_init(pmgr->load_store_ctx_pool,nb_max_ios,sizeof(struct chunk_load_store_ctx),pmgr_ctx_pool_header_size,
                     pmgr->load_store_ctx_pool+1);
    wctx->pmgr = pmgr;
    
    //reclaim manager runtime init
    rmgr->migrating_batch = opts->reclaim_batch_size;
    rmgr->reclaim_percentage_threshold = opts->reclaim_percentage_threshold;
    TAILQ_INIT(&rmgr->item_delete_head);
    TAILQ_INIT(&rmgr->item_migrate_head);
    TAILQ_INIT(&rmgr->remigrating_head);
    TAILQ_INIT(&rmgr->slab_migrate_head);

    rmgr->pending_delete_pool = (struct object_cache_pool*)(rmgr+1);
    pool_header_init(rmgr->pending_delete_pool,nb_max_ios,sizeof(struct pending_item_delete),rmgr_del_pool_header_size,
                     rmgr->pending_delete_pool+1);

    rmgr->pending_migrate_pool = (uint8_t*)rmgr->pending_delete_pool + rmgr_del_pool_header_size + 
                                 rmgr_del_pool_data_size;
    pool_header_init(rmgr->pending_migrate_pool,nb_max_ios,sizeof(struct pending_item_migrate),rmgr_mig_pool_header_size,
                     rmgr->pending_migrate_pool+1);
                                 
    rmgr->migrate_slab_pool = (uint8_t*)rmgr->pending_migrate_pool + rmgr_mig_pool_header_size +
                              rmgr_mig_pool_data_size;
    pool_header_init(rmgr->migrate_slab_pool,nb_max_ios,sizeof(struct slab_migrate_request),rmgr_slab_pool_header_size,
                     rmgr->migrate_slab_pool+1);
    wctx->rmgr = rmgr;

    //io manager runtime init
    imgr->max_pending_io = nb_max_ios;
    imgr->nb_pending_io = 0;
    imgr->read_hash.cache_hash = NULL;
    imgr->read_hash.page_hash = NULL;
    imgr->write_hash.cache_hash = NULL;
    imgr->write_hash.page_hash = NULL;
    
    imgr->cache_io_pool = (struct object_cache_pool*)(imgr+1);
    pool_header_init(imgr->cache_io_pool,nb_max_ios,sizeof(struct cache_io),imgr_cache_pool_header_size,
                     imgr->cache_io_pool+1);

    imgr->page_io_pool = (uint8_t*)imgr->cache_io_pool + imgr_cache_pool_header_size + imgr_cache_pool_data_size;
    pool_header_init(imgr->page_io_pool,nb_max_ios*2,sizeof(struct page_io),imgr_page_pool_header_size,
                     imgr->page_io_pool+1);
    wctx->imgr = imgr;

    return wctx;
}

static void
_rebuild_complete(void*ctx, int kverrno){
    struct worker_context *wctx = ctx;
    if(kverrno){
        SPDK_ERRLOG("Fails in database recovering\n");
        assert(0);
    }
    //All initialization jobs complete.
    //Register pollers to start the service.
    struct spdk_poller *poller;
    poller = spdk_poller_register(_worker_slab_evaluation_poll,wctx,DEFAULT_RECLAIM_POLLING_PERIOD_MS);
    assert(poller!=NULL);

    poller = spdk_poller_register(_worker_reclaim_poll,wctx,0);
    assert(poller!=NULL);

    poller = spdk_poller_register(_worker_business_processor_poll,wctx,0);
    assert(poller!=NULL);
}

static void
_do_worker_start(void*ctx){
    struct worker_context* wctx = ctx;

    //I should get a io channel for the io manager.
    wctx->imgr->channel = spdk_bs_alloc_io_channel(wctx->target);
    assert( wctx->imgr->channel!=NULL);

    //Perform recovering for all slab file.
    //All kv request all blocked, before the recovering completes,
    worker_perform_rebuild_async(wctx,_rebuild_complete,wctx);
}

void worker_start(struct worker_context* wctx){
    spdk_thread_send_msg(wctx->thread,_do_worker_start,wctx);
}

/************************************************************************************/
//Statistics related.
void 
worker_get_slab_statistcs(struct worker_context* ctx,struct slab_statistics **statistics_out){

}

void 
worker_get_miss_rate(struct worker_context* ctx, float *miss_rate_out){

}

void  
worker_get_nb_pending_reqs(struct worker_context* ctx, int *nb_pendings_out){

}

void 
worker_get_chunks_statistics(struct worker_context* ctx, int *nb_chunks_out,int *nb_used_chunks_out){

}
