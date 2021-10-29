
#include <pthread.h>

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/log.h"
#include "spdk/blob.h"

#include "pagechunk.h"
#include "kverrno.h"
#include "slab.h"
#include "pool.h"
#include "kvutil.h"

#include "../worker/worker_internal.h"
#include "../kvs/kvs_internal.h"


// static struct worker_context*
// _get_worker_context_from_pmgr(struct pagechunk_mgr* pmgr){
//     uint32_t i =0;

//     for(;i<g_chunkmgr_worker.nb_business_workers;i++){
//         if(g_chunkmgr_worker.wctx_array[i]->pmgr == pmgr){
//             return g_chunkmgr_worker.wctx_array[i];
//         }
//     }
//     //I will get one except the program bug.
//     //return (struct worker_context*)((uint64_t)pmgr-sizeof(struct worker_context));
//     assert(0);
//     return NULL;
// }

// static void 
// _chunkmgr_lease_one_chunk_mem(void *ctx){
//     struct chunk_miss_callback *cb_obj = ctx;
//     struct pagechunk_mgr *requestor_pmgr = cb_obj->requestor_pmgr;
//     struct pagechunk_mgr *executor_pmgr = cb_obj->executor_pmgr;

//     //I want a chunk memory, but the chunk manager tells me that I am not a busy worker
//     //Of couse, it is a bug
//     assert(requestor_pmgr!=executor_pmgr);

//     struct chunk_mem* mem = pagechunk_evict_one_chunk(executor_pmgr);
//     cb_obj->mem = mem;
//     cb_obj->kverrno = mem ? -KV_ESUCCESS : -KV_ECACHE;

//     //I get one chunk memory, but I am not the original requestor. So I should
//     //send the chunk memory to the original requestor. 
//     struct worker_context* requestor_wctx = _get_worker_context_from_pmgr(cb_obj->requestor_pmgr);
//     spdk_thread_send_msg(requestor_wctx->thread,cb_obj->finish_cb,cb_obj);
// }

// static struct worker_context* _chunkmgr_evaluate_workload(void){
//     //It is a very simple solution. I just choose the one with
//     //maximal hit rate.
//     int nb_workers = g_chunkmgr_worker.nb_business_workers;
//     uint64_t total_chunks = g_chunkmgr_worker.nb_max_chunks;

//     uint64_t min=101, max=0;
//     int min_idx=0,max_idx=0;
//     for(int i=0;i<nb_workers;i++){
//         uint64_t misses = g_chunkmgr_worker.wctx_array[i]->pmgr->miss_times;
//         uint64_t visits = g_chunkmgr_worker.wctx_array[i]->pmgr->visit_times;
//         uint64_t hit_rate = visits ? 100*(visits-misses)/visits : 0;   
//         if(hit_rate<min) {
//             min = hit_rate;
//             min_idx = i;
//         }
//         if(hit_rate>max){
//             max = hit_rate;
//             max_idx = i;
//         }
//     }

//     if(max-min<=5){
//         //I think they have basically the same hit rate. So reject it;
//         return NULL;
//     }
//     return g_chunkmgr_worker.wctx_array[max_idx];
// }

// static void
// _chunkmgr_worker_get_one_chunk_mem(void *ctx){
//     struct chunk_miss_callback *cb_obj = ctx;
//     struct worker_context* requestor_wctx = _get_worker_context_from_pmgr(cb_obj->requestor_pmgr);

//     struct chunk_mem* mem = _get_one_chunk_mem();
//     if(mem){
//         cb_obj->mem = mem;
//         cb_obj->executor_pmgr = NULL;
//         cb_obj->kverrno = -KV_ESUCCESS;

//         spdk_thread_send_msg(requestor_wctx->thread,cb_obj->finish_cb,ctx);
//     }
//     else{
//         //SPDK_NOTICELOG("Chunk mem request incomming,p_reqs:%u, mkgr_pool:%u\n", requestor_wctx->kv_request_internal_pool->nb_frees,requestor_wctx->pmgr->kv_chunk_request_pool->nb_frees);

//         //I have no any available memory. Just lease one;
//         struct worker_context* executor_wctx = _chunkmgr_evaluate_workload();
//         if(!executor_wctx || (executor_wctx==requestor_wctx)){
//              //All workers are busy. Just tell the requestor to perform LRU eviction;
//             cb_obj->mem = NULL;
//             cb_obj->executor_pmgr = cb_obj->requestor_pmgr;
//             spdk_thread_send_msg(requestor_wctx->thread,_chunkmgr_lease_one_chunk_mem,cb_obj);
//         }
//         else{
//             //Request a chunk from other worker
//             cb_obj->executor_pmgr = executor_wctx->pmgr;
//             spdk_thread_send_msg(executor_wctx->thread,
//                                   _chunkmgr_lease_one_chunk_mem,cb_obj);
//         }
//     }
// }

void meta_request_remote_pages_aysnc(struct remote_page_request *req){
    //@TODO
}

static int
_meta_stat_report(void*ctx){
    struct meta_worker_context* meta = ctx;
    int events = 0;
    int i = 0;
    int nb_workers = meta->nb_business_workers;

    for(;i<nb_workers;i++){
        uint32_t init_pages = meta->wctx_array[i]->pmgr->nb_init_pages;
        uint64_t used_pages = meta->wctx_array[i]->pmgr->nb_used_pages;
        uint64_t misses = meta->wctx_array[i]->pmgr->miss_times;
        uint64_t visits = meta->wctx_array[i]->pmgr->visit_times;
        uint64_t hit_rate = visits ? 100*(visits-misses)/visits : 0;

        //clear stats per second
        //meta.wctx_array[i]->pmgr->miss_times = 1;
        //meta.wctx_array[i]->pmgr->visit_times  = 1;

        //SPDK_NOTICELOG("chunkmgr wid:%d, chunks:%lu, total_chunks:%lu, misses:%lu, visits:%lu, hit_rate:%lu\n",i,chunks,total_chunks,misses,visits,hit_rate);
    }
    return 0;
}

/*******************************************************************/

struct slab_slide_buffer{
    uint8_t* dma_buf;
    struct slab* slab;
    uint32_t shard_id;
    uint64_t cur_slot;

    int busy;
};

struct populate_req{
    TAILQ_ENTRY(populate_req) link;

    struct kv_item *item;
    uint32_t shard_id;
    struct slab_slide_buffer* buf;

    kv_cb user_cb;
    void* user_ctx;
};

struct populate_ctx{
    struct slab_slide_buffer* buf;
    uint32_t nb_bufs;
    uint32_t nb_slabs_per_shard;
    uint32_t nb_pages_per_chunk;
    uint32_t nb_pages_per_node;
    uint32_t chunk_size;
    
    struct meta_worker_context* meta;
    TAILQ_HEAD(, populate_req) resubmit_queue;
    uint32_t nb_pendings;

    //for syncing database. 0-no sync, 1-sync requested, 2-sync submitted, 3-sync completed
    int sync_state;
    uint32_t pending_sync;
    struct populate_req* sync_req; 

    uint64_t nb_requested;
    uint64_t nb_processed;
};

struct populate_ctx g_pctx;

static void _fill_item(struct slab_slide_buffer* buf,struct populate_req* req){
    struct chunk_desc* desc = pagechunk_get_desc(buf->slab,buf->cur_slot);
    struct slab* slab = buf->slab;

    if(!desc->dma_buf){
        desc->dma_buf = malloc(sizeof(*desc->dma_buf));
        desc->dma_buf->dma_base = buf->dma_buf;
    }
    //printf("fill slot,slab:%u,slot:%lu\n",slab->slab_size,slot_idx);
    //fflush(stdout);
    
    pagechunk_put_item(NULL,desc,buf->cur_slot,req->item);
    buf->cur_slot++;

    req->user_cb(req->user_ctx,NULL,0);
    free(req);

    g_pctx.nb_pendings--;
    g_pctx.nb_processed++;
}

static void _sync_cb(void*ctx, int kverrno){
    struct populate_req* req = ctx;
    assert(!kverrno); 

    struct slab_slide_buffer* buf = req->buf;
    struct slab* slab = buf->slab;
    uint32_t total_nodes = slab->reclaim.nb_reclaim_nodes+1; 

    buf->busy = 0;

    if(total_nodes>=slab->reclaim.cap){
        //in case of resizing
        uint64_t tmp = slab->reclaim.cap*2;
        slab->reclaim.cap = tmp < total_nodes ? total_nodes : tmp;
        slab->reclaim.node_array = realloc(slab->reclaim.node_array,slab->reclaim.cap*sizeof(void*));
        assert(slab->reclaim.node_array);
    }

    struct reclaim_node* node = slab_reclaim_alloc_one_node(slab,slab->reclaim.nb_reclaim_nodes);
    slab->reclaim.nb_total_slots += node->nb_free_slots;
    slab->reclaim.nb_free_slots += node->nb_free_slots;

    //rbtree_insert(slab->reclaim.free_node_tree,node->id,node,NULL);
    slab->reclaim.node_array[slab->reclaim.nb_reclaim_nodes] = node;
    slab->reclaim.nb_reclaim_nodes++; 

    _fill_item(buf,req);   
}

static void _write_zero_cb(void*ctx, int kverrno){
    struct populate_req* req = ctx;
    assert(!kverrno);

    struct slab_slide_buffer* buf = req->buf;

    spdk_blob_sync_md(buf->slab->blob,_sync_cb,req);
}

static void _resize_cb(void*ctx, int kverrno){
    struct populate_req* req = ctx;
    assert(!kverrno);

    struct slab_slide_buffer* buf = req->buf;
    struct slab* slab = buf->slab;

    uint64_t off = slab->reclaim.nb_reclaim_nodes*g_pctx.nb_pages_per_node;
    uint64_t len = g_pctx.nb_pages_per_node;
    
    //SPDK_NOTICELOG("Wrire zeroes,blob:%p, slab:%u,off:%lu,len:%lu\n",slab->blob,slab->slab_size,off,len);
    spdk_blob_io_write_zeroes(buf->slab->blob,g_pctx.meta->channel,off,len,_write_zero_cb,req);
}

static void _buf_flush_cb(void*ctx, int kverrno){
    struct populate_req* req = ctx;
    assert(!kverrno);

    struct slab_slide_buffer* buf = req->buf;
    struct slab* slab = buf->slab;

    buf->busy = 0;

    uint64_t nb_slots_per_node = slab->reclaim.nb_chunks_per_node*
                                    slab->reclaim.nb_slots_per_chunk;
    uint64_t slots_per_chunk =  slab->reclaim.nb_slots_per_chunk;
    uint64_t node_id = buf->cur_slot/nb_slots_per_node;

    if(node_id<slab->reclaim.nb_reclaim_nodes){
        _fill_item(buf,req);
    }
    else{
        if(buf->busy){
            TAILQ_INSERT_TAIL(&g_pctx.resubmit_queue,req,link);
            return;
        }
        //allocate new node
        uint64_t cur_sz = slab->reclaim.nb_reclaim_nodes*slab->reclaim.nb_chunks_per_node;
        uint64_t nb_sz_per_node = slab->reclaim.nb_chunks_per_node;
        uint64_t new_sz = cur_sz + nb_sz_per_node;

        buf->busy = 1;
        spdk_blob_resize(slab->blob,new_sz,_resize_cb,req);
    }
}

static void _process_one_populate_request(struct populate_req* req){

    if(!req->item){
        //It is a sync command.
        assert(!g_pctx.sync_state);
        g_pctx.sync_state = 1; //sync requested
        g_pctx.sync_req = req;
        g_pctx.nb_pendings--;
        g_pctx.nb_processed++;
        return;
    }

    uint32_t shard_id = req->shard_id;
    uint32_t slab_id = slab_find_slab(item_packed_size(req->item));
    uint32_t buf_id = shard_id*g_pctx.nb_slabs_per_shard + slab_id;
    struct slab_slide_buffer* buf = &g_pctx.buf[buf_id];
    struct slab* slab = buf->slab;
    req->buf = buf;

    uint64_t nb_slots_per_node = slab->reclaim.nb_chunks_per_node*
                                    slab->reclaim.nb_slots_per_chunk;
    uint64_t slots_per_chunk =  slab->reclaim.nb_slots_per_chunk;

    if(buf->cur_slot==0 || buf->cur_slot%slots_per_chunk!=0){
        _fill_item(buf,req);
    }
    else{
        if(buf->busy){
            TAILQ_INSERT_TAIL(&g_pctx.resubmit_queue,req,link);
            return;
        }
        //flush the chunk to disk
        uint64_t node_id = buf->cur_slot/nb_slots_per_node;
        uint64_t chunk_id = (buf->cur_slot%nb_slots_per_node)/slots_per_chunk - 1;

        uint64_t off = node_id*g_pctx.nb_pages_per_node + chunk_id*g_pctx.nb_pages_per_chunk;
        uint64_t len = g_pctx.nb_pages_per_chunk;
        buf->busy = 1;

        spdk_blob_io_write(buf->slab->blob,g_pctx.meta->channel,buf->dma_buf,off,len,_buf_flush_cb,req);
    }
}

static void _buf_sync_cb(void*ctx, int kverrno){
    assert(!kverrno);

    struct slab_slide_buffer* buf = ctx;
    g_pctx.pending_sync--;

    printf("syncing shard:%u,slab:%u, slots:%lu complete\n",buf->shard_id,buf->slab->slab_size,buf->cur_slot);

    if(!g_pctx.pending_sync){
        g_pctx.sync_state = 3;
    }
}

static void _process_db_sync(void){
    //There should be no more requests since the db is in sync state
    assert(g_pctx.sync_req);

    //do db sync.
    if(g_pctx.nb_pendings){
        //wait for the completion of rest requests.
        return 0;
    }

    if(g_pctx.sync_state == 1){
        g_pctx.pending_sync = g_pctx.nb_bufs;

        for(uint32_t i=0;i<g_pctx.nb_bufs;i++){
            assert(!g_pctx.buf[i].busy);
            if(g_pctx.buf[i].cur_slot){
                //It is an active slab, flush the last 
                struct slab* slab = g_pctx.buf[i].slab;
                uint64_t nb_slots_per_node = slab->reclaim.nb_chunks_per_node*
                                             slab->reclaim.nb_slots_per_chunk;
                uint64_t slots_per_chunk =  slab->reclaim.nb_slots_per_chunk;

                uint64_t node_id = g_pctx.buf[i].cur_slot/nb_slots_per_node;
                uint64_t chunk_id = (g_pctx.buf[i].cur_slot%nb_slots_per_node)/slots_per_chunk;

                uint64_t off = node_id * g_pctx.nb_pages_per_node +
                               chunk_id* g_pctx.nb_pages_per_chunk;
                uint64_t len = g_pctx.nb_pages_per_chunk;
                spdk_blob_io_write(g_pctx.buf[i].slab->blob,g_pctx.meta->channel,g_pctx.buf[i].dma_buf,off,len,_buf_sync_cb,&g_pctx.buf[i]);
            }
            else{
                //No data for the buf, just ignore it.
                g_pctx.pending_sync--;
            }
        }
        g_pctx.sync_state = 2; //sync submitted
        return 0;
    }


    if(g_pctx.sync_state == 2){
        //Wait the completion of db sync
        return 0;
    }

    if(g_pctx.sync_state==3){
        //sync complete
        g_pctx.sync_req->user_cb(g_pctx.sync_req->user_ctx,NULL,0);
        printf("total requested:%lu, processed:%lu\n",g_pctx.nb_requested,g_pctx.nb_processed);
        g_pctx.sync_state=4;
        return 0;
    }
}

static int _do_populate_item(void*ctx){
    struct meta_worker_context* meta = ctx;

    uint32_t p_req = spdk_ring_count(meta->populate_ring);
    struct populate_req* req_arr[p_req];

    /**
     * @brief Process the resubmit queue. I needn't get new cached requests
     * from requests pool, since requests from resubmit queue already have 
     * cached request object.
     * Just link then to submit queue.
     */
    struct populate_req *req,*tmp=NULL;
    TAILQ_FOREACH_SAFE(req,&g_pctx.resubmit_queue,link,tmp){
        if(!req->buf->busy){
            TAILQ_REMOVE(&g_pctx.resubmit_queue,req,link);
            _process_one_populate_request(req);
        }
    }

    if(g_pctx.nb_pendings>1024){
        return 0;
    }

    if(g_pctx.sync_state){
        assert(!p_req);
        _process_db_sync();
        return 0;
    }

    if(p_req){
        uint64_t res = spdk_ring_dequeue(meta->populate_ring,(void**)&req_arr,p_req);
        assert(res==p_req);
    }

    for(uint32_t i=0;i<p_req;i++){
        g_pctx.nb_pendings++;
        _process_one_populate_request(req_arr[i]);
        g_pctx.nb_requested++;
    }

    return p_req;
}

static inline uint32_t
_hash_item_to_shard(struct kv_item *item){
    uint64_t prefix = *(uint64_t*)item->data;
    prefix =  (((uint64_t)htonl(prefix))<<32) + htonl(prefix>>32); 
    return prefix%g_kvs->nb_shards;
}

void kv_populate_async(struct kv_item *item, kv_cb cb_fn, void* ctx){
    assert(g_kvs);

    struct populate_req* req = malloc(sizeof(*req));
    req->item = item;
    req->shard_id = item ? _hash_item_to_shard(item) : 0;
    req->user_cb = cb_fn;
    req->user_ctx = ctx;

    while(spdk_ring_enqueue(g_kvs->meta_worker->populate_ring,(void**)&req,1,NULL)!=1 ){
        spdk_pause();
    }
}

/*******************************************************************/

struct meta_worker_context* meta_worker_alloc(struct meta_init_opts *opts){
    struct meta_worker_context* meta = malloc(sizeof(struct meta_worker_context));
    assert(meta);

    meta->nb_business_workers = opts->nb_business_workers;
    meta->wctx_array = opts->wctx_array;

    uint32_t core_id = spdk_env_get_current_core();
    assert(core_id==opts->core_id);
    meta->core_id = core_id;
    meta->meta_thread = opts->meta_thread;
    meta->channel = opts->channel;

    //for populate db
    meta->populate_ring = spdk_ring_create(SPDK_RING_TYPE_MP_SC,1024,SPDK_ENV_SOCKET_ID_ANY);

    uint32_t nb_shards = opts->nb_shards;
    uint32_t nb_slabs_per_shard = opts->shards[0].nb_slabs;
    uint32_t chunk_size = opts->shards[0].slab_set[0].reclaim.nb_pages_per_chunk*KVS_PAGE_SIZE;

    g_pctx.nb_bufs = nb_shards*nb_slabs_per_shard;
    g_pctx.nb_slabs_per_shard = opts->shards[0].nb_slabs;
    g_pctx.chunk_size = chunk_size;
    g_pctx.nb_pages_per_chunk = opts->shards[0].slab_set[0].reclaim.nb_pages_per_chunk;
    g_pctx.nb_pages_per_node = g_pctx.nb_pages_per_chunk*opts->shards[0].slab_set[0].reclaim.nb_chunks_per_node;
    g_pctx.buf = malloc(sizeof(struct slab_slide_buffer)*g_pctx.nb_bufs);
    g_pctx.meta = meta;

    g_pctx.nb_pendings = 0;
    TAILQ_INIT(&g_pctx.resubmit_queue);

    g_pctx.sync_state = 0;
    g_pctx.pending_sync = 0;
    g_pctx.sync_req = NULL;

    g_pctx.nb_requested = 0;
    g_pctx.nb_processed = 0;

    for(uint32_t i=0;i<g_pctx.nb_bufs;i++){
        uint32_t shard_id = i/nb_slabs_per_shard;
        uint32_t slab_id = i%nb_slabs_per_shard;

        struct slab* slab = &opts->shards[shard_id].slab_set[slab_id];
        g_pctx.buf[i].cur_slot = 0;
        g_pctx.buf[i].slab = slab;
        g_pctx.buf[i].dma_buf = spdk_dma_zmalloc(chunk_size,0x1000,NULL);
        g_pctx.buf[i].busy = 0;
        g_pctx.buf[i].shard_id = shard_id;
    }

    return meta;
}

void meta_worker_start(struct meta_worker_context* meta){
    assert(meta);
    assert(meta->meta_thread);
    meta->stat_poller = SPDK_POLLER_REGISTER(_meta_stat_report,meta,1000000); //report every 1s
    meta->populate_poller = SPDK_POLLER_REGISTER(_do_populate_item,meta,0);
    SPDK_NOTICELOG("chunkmgr thread is working\n");
}

void meta_worker_destroy(struct meta_worker_context* meta){
    assert(meta);
    assert(meta->meta_thread);
    spdk_thread_exit(meta->meta_thread);
    spdk_thread_destroy(meta->meta_thread);

    free(meta);
}

