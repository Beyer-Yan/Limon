
#include <pthread.h>

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/log.h"
#include "pagechunk.h"
#include "kverrno.h"
#include "slab.h"
#include "pool.h"
#include "kvutil.h"

#include "../worker/worker_internal.h"


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

struct meta_worker_context* meta_worker_alloc(struct meta_init_opts *opts){
    struct meta_worker_context* meta = malloc(sizeof(struct meta_worker_context));
    assert(meta);

    meta->nb_business_workers = opts->nb_business_workers;
    meta->wctx_array = opts->wctx_array;

    uint32_t core_id = spdk_env_get_current_core();
    assert(core_id==opts->core_id);
    meta->core_id = core_id;
    meta->meta_thread = spdk_get_thread();
    return meta;
}

void meta_worker_start(struct meta_worker_context* meta){
    assert(meta);
    assert(meta->meta_thread);
    meta->stat_poller = SPDK_POLLER_REGISTER(_meta_stat_report,meta,1000000); //report every 1s
    SPDK_NOTICELOG("chunkmgr thread is working\n");
}

void meta_worker_destroy(struct meta_worker_context* meta){
    assert(meta);
    assert(meta->meta_thread);
    spdk_thread_exit(meta->meta_thread);
    spdk_thread_destroy(meta->meta_thread);

    free(meta);
}

