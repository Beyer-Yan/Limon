#include "worker_internal.h"
#include "kverrno.h"
#include "slab.h"
#include "rbtree_uint.h"
#include "kvutil.h"
#include "hashmap.h"

#include "spdk/log.h"

//10s
#define DEFAULT_RECLAIM_POLLING_PERIOD_US 10000000

#define CONFLICTS_CAPACITY 1024

//A simple read-write "lock"
struct req_conflict{
    struct{
        uint16_t reads:15;
        uint16_t write:1;
    }pendings[CONFLICTS_CAPACITY];
};

struct req_conflict* conflict_new(void){
    struct req_conflict* conflicts = calloc(1,sizeof(struct req_conflict));
    assert(conflicts);
    return conflicts;
}

void conflict_destroy(struct req_conflict* conflicts){
    assert(conflicts);
    free(conflicts);
}

uint32_t conflict_check_or_enter(struct req_conflict* conflicts, struct kv_item *item,enum op_code op){
    assert(conflicts);
    assert(item);
    //reserve bucket 0 
    uint32_t bucket = kv_hash(item->data,item->meta.ksize,CONFLICTS_CAPACITY-1)+1;
    int res = 0;

    if(op==GET){
        if(conflicts->pendings[bucket].write){
            //There is a pending write for this slot
            res = 1;
        }else{
            //No conflicts between reads
            conflicts->pendings[bucket].reads++;
        }
    }else{
        //Write have conflict with both write and read.
        if(conflicts->pendings[bucket].write || conflicts->pendings[bucket].reads){
            res = 1;
        }else{
            conflicts->pendings[bucket].write = 1;
        }
    }
    return res ? 0 : bucket;
}

void conflict_leave(struct req_conflict* conflicts, uint32_t bucket,enum op_code op){
    assert(conflicts);

    if(op==GET){
        assert(conflicts->pendings[bucket].write==0);
        assert(conflicts->pendings[bucket].reads);
        conflicts->pendings[bucket].reads--;
    }else{
        assert(conflicts->pendings[bucket].write==1);
        conflicts->pendings[bucket].write = 0;
    }
}   

static void 
_process_one_kv_request(struct worker_context *wctx, struct kv_request_internal *req){
    req->pctx.wctx = wctx;

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
        default:
            pool_release(wctx->kv_request_internal_pool,req);
            req->cb_fn(req->ctx,NULL,-KV_EOP_UNKNOWN);
            break;
    }
}

static int
_worker_poll_business(struct worker_context *wctx){
    int events = 0;

    // The pending requests in the worker context no-lock request queue
    uint32_t p_reqs = spdk_ring_count(wctx->req_ring);

    // The avalaible pending requests in the worker request pool
    uint32_t a_reqs = wctx->kv_request_internal_pool->nb_frees;
    uint32_t process_size = p_reqs < a_reqs ? p_reqs : a_reqs;

    //SPDK_NOTICELOG("p_reqs:%u, a_reqs:%u, process:%u\n",p_reqs,a_reqs, process_size);

    struct kv_request_internal *req_internal,*tmp=NULL;
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
     * @brief Batch process the submit queue. For every kv request from users, I have to
     * get a request_internal object from request object pool to wrap it.
     */
    if(process_size>0){
        struct kv_request *req_array[wctx->max_pending_kv_request];
        uint64_t res = spdk_ring_dequeue(wctx->req_ring,(void**)&req_array,process_size);
        assert(res==process_size);
        
        uint32_t i = 0;
        for(;i<process_size;i++){
            req_internal = pool_get(wctx->kv_request_internal_pool);
            assert(req_internal!=NULL);

            req_internal->shard = req_array[i]->shard;
            req_internal->item = req_array[i]->item;
            req_internal->op_code = req_array[i]->op_code;
            req_internal->sid = req_array[i]->sid;
            req_internal->cb_fn = req_array[i]->cb_fn;
            req_internal->ctx = req_array[i]->ctx;

            TAILQ_INSERT_TAIL(&wctx->submit_queue,req_internal,link);
            free(req_array[i]);
        }
    }
 
    TAILQ_FOREACH_SAFE(req_internal, &wctx->submit_queue,link,tmp){
        TAILQ_REMOVE(&wctx->submit_queue,req_internal,link);
        _process_one_kv_request(wctx, req_internal);
        events++;
    }

    return events;
}

static int
_worker_poll_io(void* ctx){
    struct worker_context *wctx = ctx;
    int events = 0;
    events += iomgr_io_poll(wctx->imgr);
    return events;
}

static int
_worker_poll_reclaim(struct worker_context *wctx){
    int events = 0;
    events += worker_reclaim_process_pending_slab_migrate(wctx);
    events += worker_reclaim_process_pending_item_migrate(wctx);
    return events;
}

static int
_worker_request_poll(void*ctx){
    struct worker_context *wctx = ctx;
    int events = 0;
    events += _worker_poll_reclaim(wctx);
    events += _worker_poll_business(wctx);
    return events;
}
static void
_fill_slab_migrate_req(struct slab_migrate_request *req, struct slab* slab ){
    //Submit a slab shrinking request
    req->slab = slab;
    req->is_fault = false;

    //Get the node with the maximal fragmentation
    uint32_t i = 0, maxVal=0, idx=0;
    for(;i<slab->reclaim.nb_reclaim_nodes;i++){
        if(slab->reclaim.node_array[i]->nb_free_slots>maxVal){
            maxVal = slab->reclaim.node_array[i]->nb_free_slots;
            idx = i;
        }
    }

    //Test. Simulate to always trigger the first node
    idx = 0;

    uint64_t nb_total_slots = slab->reclaim.nb_chunks_per_node*slab->reclaim.nb_slots_per_chunk;

    req->node = slab->reclaim.node_array[idx];
    req->nb_processed = 0;
    req->nb_valid_slots = nb_total_slots - req->node->nb_free_slots;
    req->nb_faults = 0;
    req->start_slot = slab_reclaim_get_start_slot(&slab->reclaim,req->node);
    req->cur_slot = req->start_slot;
    req->last_slot = req->start_slot + slab->reclaim.nb_slots_per_chunk * slab->reclaim.nb_chunks_per_node - 1;

    //The node is not allowed to performing allocating.
    //If it is not in the free_node_tree, the deleting will no nothing.
    //All the free slots in this node will be cleared.
    rbtree_delete(slab->reclaim.free_node_tree,req->node->id,NULL);
    slab->reclaim.nb_free_slots -= req->node->nb_free_slots;
}

//This poller shall be excecuted regularly, e.g. once a second.
static int
_worker_slab_evaluation_poll(void* ctx){
    struct worker_context *wctx = ctx;
    uint32_t i=0,j=0;

    int events = 0;
    return 0;

    for(;i < wctx->nb_reclaim_shards;i++){
        struct slab_shard *shard = &wctx->shards[wctx->reclaim_shards_start_id + i];
        for(j=0;j < shard->nb_slabs;j++){
            if( !(shard->slab_set[j].flag & SLAB_FLAG_RECLAIMING) ){
                struct slab* slab = &(shard->slab_set[j]);
                if( slab_reclaim_evaluate_slab(slab) ){
                    struct slab_migrate_request *slab_migrate_req = pool_get(wctx->rmgr->migrate_slab_pool);
                    assert(slab_migrate_req!=NULL);

                    _fill_slab_migrate_req(slab_migrate_req,slab);
                    slab_migrate_req->wctx = wctx;
                    slab->flag |= SLAB_FLAG_RECLAIMING;
                    TAILQ_INSERT_TAIL(&wctx->rmgr->slab_migrate_head,slab_migrate_req,link);

                    events++;
                }
            }
        }
    }
    return events;
}

static struct kv_request*
_get_free_req_buffer(struct worker_context* wctx){
   return malloc(sizeof(struct kv_request));
}

static void
_submit_req_buffer(struct worker_context* wctx,struct kv_request *req){
    while(spdk_ring_enqueue(wctx->req_ring,(void**)&req,1,NULL)!=1 ){
        spdk_pause();
    }
}

static inline void
_worker_enqueue_common(struct kv_request* req, uint32_t shard,
                       struct kv_item *item, uint64_t sid,
                       kv_cb cb_fn,void* ctx,
                       enum op_code op){
    
    //For debug only
    if(op==GET || op==PUT){
        uint64_t tsc;
        rdtscll(tsc);
        memcpy(item->meta.cdt,&tsc,sizeof(tsc));
    }

    req->cb_fn = cb_fn;
    req->ctx = ctx;
    req->item = item;
    req->sid = sid;
    req->op_code = op;
    req->shard = shard;
}

void worker_enqueue_get(struct worker_context* wctx,struct kv_item *item,uint64_t sid, kv_cb cb_fn, void* ctx){

    assert(wctx!=NULL);
    struct kv_request *req = _get_free_req_buffer(wctx);

    //the shard and item field will be ignored
    _worker_enqueue_common(req,0,item,sid,cb_fn,ctx,GET);
    _submit_req_buffer(wctx,req);
}

void worker_enqueue_put(struct worker_context* wctx,uint32_t shard,
                        struct kv_item *item, uint64_t sid,
                        kv_cb cb_fn, void* ctx){
    assert(wctx!=NULL);
    struct kv_request *req = _get_free_req_buffer(wctx);
    _worker_enqueue_common(req,shard,item,sid,cb_fn,ctx,PUT);
    _submit_req_buffer(wctx,req);
}

void worker_enqueue_delete(struct worker_context* wctx,
                           struct kv_item *item, uint64_t sid, 
                           kv_cb cb_fn, void* ctx){
    assert(wctx!=NULL);
    struct kv_request *req = _get_free_req_buffer(wctx);

    //the shard and item field will be ignored
    _worker_enqueue_common(req,0,item,sid,cb_fn,ctx,DELETE);
    _submit_req_buffer(wctx,req);
}

static struct pagechunk_mgr*
_alloc_pmgr_context(struct worker_init_opts* opts){
    uint64_t nb_max_reqs = opts->max_request_queue_size_per_worker;  

    struct pagechunk_mgr* pmgr = malloc(sizeof(*pmgr));
    assert(pmgr);

    TAILQ_INIT(&pmgr->pages_head);
    pmgr->nb_init_pages = opts->nb_init_pages;
    pmgr->nb_used_pages = 0;
    pmgr->water_mark  = opts->chunk_cache_water_mark;
    pmgr->visit_times = 0;
    pmgr->miss_times = 0;
    pmgr->seed = rand();

    pmgr->page_map = hashmap_new();
    assert(pmgr->page_map);

    pmgr->meta_thread = opts->meta_thread;

    pmgr->remote_page_request_pool = pool_create(nb_max_reqs,sizeof(struct remote_page_request));
    
    uint32_t max_pages_per_req = MAX_SLAB_SIZE/KVS_PAGE_SIZE/2+1;
    uint32_t max_load_stores = (nb_max_reqs+opts->reclaim_batch_size)*max_pages_per_req;
    pmgr->load_store_ctx_pool = pool_create(max_load_stores,sizeof(struct page_load_store_ctx));
    pmgr->item_ctx_pool = pool_create(nb_max_reqs+opts->reclaim_batch_size,sizeof(struct item_load_store_ctx));

    assert(pmgr->remote_page_request_pool);
    assert(pmgr->load_store_ctx_pool);
    assert(pmgr->item_ctx_pool);

    uint64_t cache_size = pmgr->nb_init_pages*KVS_PAGE_SIZE;
    SPDK_NOTICELOG("Allocating page cache memory, worker:%u, size:%lu pages\n",opts->core_id,pmgr->nb_init_pages);

    uint8_t* cache_addr = malloc(cache_size);
    struct page_desc* pdesc_arr = malloc(pmgr->nb_init_pages*sizeof(struct page_desc));
    assert(cache_addr);
    assert(pdesc_arr);

    for(uint64_t i=0;i<pmgr->nb_init_pages;i++){
        pdesc_arr[i].data = cache_addr + i*KVS_PAGE_SIZE;
    }
    pmgr->cache_base_addr = cache_addr;
    pmgr->pdesc_arr = pdesc_arr;

    //allocate the DMA buffer
    uint32_t nb_dma_buffers = nb_max_reqs+opts->reclaim_batch_size;
    pmgr->dma_pool = dma_buffer_pool_create(nb_dma_buffers,CHUNK_PAGES);

    assert(pmgr->dma_pool);

    return pmgr;
}

static struct reclaim_mgr*
_alloc_rmgr_context(struct worker_init_opts* opts){
    uint32_t nb_max_slab_reclaim = opts->nb_reclaim_shards*opts->shard[0].nb_slabs;

    struct reclaim_mgr* rmgr = malloc(sizeof(struct reclaim_mgr));
    assert(rmgr);

    rmgr->migrating_batch = opts->reclaim_batch_size;
    rmgr->reclaim_percentage_threshold = opts->reclaim_percentage_threshold;
    rmgr->nb_pending_slots = 0;

    TAILQ_INIT(&rmgr->item_migrate_head);
    TAILQ_INIT(&rmgr->remigrating_head);
    TAILQ_INIT(&rmgr->slab_migrate_head);

    rmgr->pending_migrate_pool = pool_create(rmgr->migrating_batch,sizeof(struct pending_item_migrate));
    rmgr->migrate_slab_pool = pool_create(nb_max_slab_reclaim,sizeof(struct slab_migrate_request));

    assert(rmgr->pending_migrate_pool);
    assert(rmgr->migrate_slab_pool);

    return rmgr;
}

static struct iomgr*
_alloc_imgr_context(struct worker_init_opts* opts){
    uint64_t nb_max_reqs = opts->max_request_queue_size_per_worker;

    struct iomgr* imgr = malloc(sizeof(struct iomgr));
    assert(imgr);

    imgr->meta_thread = opts->meta_thread;
    imgr->target = opts->target;
    imgr->max_pending_io = opts->max_request_queue_size_per_worker;
    imgr->nb_pending_io = 0;
    imgr->io_unit_size = spdk_bs_get_io_unit_size(opts->target);

    imgr->read_hash.cache_hash  = hashmap_new();
    imgr->read_hash.page_hash   = hashmap_new();
    imgr->write_hash.cache_hash = hashmap_new();
    imgr->write_hash.page_hash  = hashmap_new();

    assert(imgr->read_hash.cache_hash);
    assert(imgr->read_hash.page_hash);
    assert(imgr->write_hash.cache_hash);
    assert(imgr->write_hash.page_hash);
    
    TAILQ_INIT(&imgr->pending_read_head);
    TAILQ_INIT(&imgr->pending_write_head);

    uint32_t max_pages_per_req = MAX_SLAB_SIZE/KVS_PAGE_SIZE/2+1;
    uint32_t max_cache_reqs = (nb_max_reqs+opts->reclaim_batch_size)*max_pages_per_req;
    imgr->cache_io_pool = pool_create(max_cache_reqs,sizeof(struct cache_io));
    imgr->page_io_pool = pool_create(max_cache_reqs*2,sizeof(struct page_io));

    assert(imgr->cache_io_pool);
    assert(imgr->page_io_pool);

    return imgr;
}

struct worker_context* 
worker_alloc(struct worker_init_opts* opts)
{
    struct worker_context *wctx = malloc(sizeof(*wctx));
    assert(wctx!=NULL);

    uint32_t nb_max_reqs = opts->max_request_queue_size_per_worker;

    wctx->ready = false;
    wctx->shards = opts->shard;
    wctx->nb_shards = opts->nb_shards;
    wctx->core_id = opts->core_id;

    wctx->global_index = opts->global_index;

    wctx->nb_reclaim_shards = opts->nb_reclaim_shards;
    wctx->reclaim_shards_start_id = opts->reclaim_shard_start_id;
    wctx->reclaim_percentage_threshold = opts->reclaim_percentage_threshold;
    wctx->io_cycle = opts->io_cycle;
    wctx->max_pending_kv_request = nb_max_reqs;

    wctx->kv_request_internal_pool = pool_create(nb_max_reqs,sizeof(struct kv_request_internal));
    assert(wctx->kv_request_internal_pool);

    wctx->req_ring = spdk_ring_create(SPDK_RING_TYPE_MP_SC,nb_max_reqs,SPDK_ENV_SOCKET_ID_ANY);
    assert(wctx->req_ring!=NULL);

    TAILQ_INIT(&wctx->submit_queue);
    TAILQ_INIT(&wctx->resubmit_queue);

    wctx->add_conflicts = conflict_new();
    assert(wctx->add_conflicts);

    wctx->mtable = mtable_new(opts->core_id,nb_max_reqs+opts->reclaim_batch_size);

    wctx->pmgr = _alloc_pmgr_context(opts);
    wctx->rmgr = _alloc_rmgr_context(opts);
    wctx->imgr = _alloc_imgr_context(opts);

    struct spdk_cpuset cpumask;
    //The name will be copied. So the stack variable makes sense.
    char thread_name[32];

    spdk_cpuset_zero(&cpumask);
    spdk_cpuset_set_cpu(&cpumask,opts->core_id,true);
    snprintf(thread_name,sizeof(thread_name),"worker_%u",opts->core_id);

    wctx->core_id = opts->core_id;
    wctx->thread = spdk_thread_create(thread_name,&cpumask);
    //wctx->thread = spdk_thread_create(thread_name,NULL);
    assert(wctx->thread!=NULL);
    return wctx;
}

static void _pmgr_destroy(struct pagechunk_mgr *pmgr){
    assert(pmgr);
    free(pmgr->cache_base_addr);
    free(pmgr->pdesc_arr);
    
    dma_buffer_pool_destroy(pmgr->dma_pool);
    pool_destroy(pmgr->remote_page_request_pool);
    pool_destroy(pmgr->load_store_ctx_pool);
    pool_destroy(pmgr->item_ctx_pool);

    free(pmgr);
}

static void _rmgr_destroy(struct reclaim_mgr *rmgr){
    assert(rmgr);

    free(rmgr->migrate_slab_pool);
    free(rmgr->pending_migrate_pool);
    free(rmgr);
}

static void _imgr_destroy(struct iomgr *imgr){
    assert(imgr);

    free(imgr->cache_io_pool);
    free(imgr->page_io_pool);

    hashmap_free(imgr->read_hash.cache_hash);
    hashmap_free(imgr->read_hash.page_hash);
    hashmap_free(imgr->write_hash.cache_hash);
    hashmap_free(imgr->write_hash.page_hash);

    free(imgr);
}

static void
_rebuild_complete(void*ctx, int kverrno){
    struct worker_context *wctx = ctx;
    if(kverrno){
        SPDK_ERRLOG("Fails in database recovering,wid:%u\n",wctx->core_id);
        assert(0);
    }
    SPDK_NOTICELOG("Rebuild completes, wid:%u\n",wctx->core_id);
    //All initialization jobs complete.
    //Register pollers to start the service.
    struct spdk_poller *poller;

    poller = SPDK_POLLER_REGISTER(_worker_request_poll,wctx,0);
    assert(poller!=NULL); 
    wctx->request_poller = poller;

    poller = SPDK_POLLER_REGISTER(_worker_poll_io,wctx,wctx->io_cycle);
    assert(poller!=NULL);
    wctx->io_poller = poller;

    poller = SPDK_POLLER_REGISTER(_worker_slab_evaluation_poll,wctx,DEFAULT_RECLAIM_POLLING_PERIOD_US);
    assert(poller!=NULL);
    wctx->slab_evaluation_poller = poller;

    wctx->ready = true;
}

static void
_do_worker_start(void*ctx){
    struct worker_context* wctx = ctx;

    //I should get a io channel for the io manager.
    wctx->imgr->channel = spdk_bs_alloc_io_channel(wctx->imgr->target);
    assert( wctx->imgr->channel!=NULL);

    //Perform recovering for all slab file.
    //All kv request all blocked, before the recovering completes,
    SPDK_NOTICELOG("Start rebuilding,wid:%u\n",wctx->core_id);
    worker_perform_rebuild_async(wctx,_rebuild_complete,wctx);
}

void worker_start(struct worker_context* wctx){
    assert(wctx!=NULL);
    spdk_thread_send_msg(wctx->thread,_do_worker_start,wctx);
}

bool worker_is_ready(struct worker_context* wctx){
    return wctx->ready;
}

static void
_do_worker_destroy_check_pollers(void*ctx){
    struct worker_context* wctx = ctx;
    if(spdk_thread_has_active_pollers(wctx->thread)){
        SPDK_ERRLOG("Error in bdev poller unregister\n");
    }
    else{
        spdk_thread_exit(wctx->thread);
        //spdk_thread_destroy(wctx->thread);

        //Free all the memory
        spdk_ring_free(wctx->req_ring);
        pool_destroy(wctx->kv_request_internal_pool);
        conflict_destroy(wctx->add_conflicts);
        mtable_destroy(wctx->mtable);

        _pmgr_destroy(wctx->pmgr);
        _rmgr_destroy(wctx->rmgr);
        _imgr_destroy(wctx->imgr);

        //Release the worker context memory.
        SPDK_NOTICELOG("Worker has been destroyed,w:%d\n",wctx->core_id);
        free(wctx);
    }
}

static void
_do_worker_destroy(void*ctx){
    struct worker_context* wctx = ctx;
    //Release all the poller.
    spdk_poller_unregister(&wctx->request_poller);
    spdk_poller_unregister(&wctx->io_poller);
    spdk_poller_unregister(&wctx->slab_evaluation_poller);

    spdk_put_io_channel(wctx->imgr->channel);
    spdk_thread_send_msg(wctx->thread,_do_worker_destroy_check_pollers,wctx);
}

void worker_destroy(struct worker_context* wctx){
    assert(wctx!=NULL);
    spdk_thread_send_msg(wctx->thread,_do_worker_destroy,wctx);
}

/************************************************************************************/
//Statistics related.

void worker_get_statistics(struct worker_context* wctx, struct worker_statistics* ws_out){
    assert(wctx!=NULL);
    ws_out->chunk_miss_times = wctx->pmgr->miss_times;
    ws_out->chunk_hit_times  = wctx->pmgr->visit_times;
    ws_out->nb_pending_reqs  = spdk_ring_count(wctx->req_ring);
    ws_out->nb_pending_ios   = wctx->imgr->nb_pending_io;
    ws_out->nb_used_pages   = wctx->pmgr->nb_used_pages;
}
