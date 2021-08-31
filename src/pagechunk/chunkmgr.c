
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

//Sinleton mode
static struct chunkmgr_worker_context g_chunkmgr_worker;

static uint8_t* g_mem_desc  = NULL;
static uint32_t g_desc_size = 0;
static uint64_t g_total_chunks = 0;
static uint64_t g_cur = 0;

struct spdk_poller *stat_poller = NULL;

//static pthread_mutex_t g_mutex;

#define GB(x) ((x)*1024u*1024u*1024u)

//Allocate chunk memory in one-shot mode.
static void
_chunk_mem_init(uint64_t nb_chunks){

    //allocate all chunk memory descriptor
    uint64_t bitmap_size = bitmap_header_size(g_chunkmgr_worker.nb_pages_per_chunk);
    uint64_t mem_desc_size = KV_ALIGN(sizeof(struct chunk_mem)+bitmap_size,8);
    
    g_mem_desc = calloc(nb_chunks,mem_desc_size);
    assert(g_mem_desc && "memory allocation failed");

    g_desc_size = mem_desc_size;
    g_total_chunks = nb_chunks;
    g_cur = 0;

    //allocator all chunk memory. I have to allocate memories for many times because
    //the SPDK fails to give a very large memory in one memory allocation.
    uint64_t chunk_data_size = g_chunkmgr_worker.nb_pages_per_chunk * KVS_PAGE_SIZE;
    uint64_t total_size = chunk_data_size*nb_chunks;

    assert(GB(1)%chunk_data_size==0 && "chunk size shall be the size with the power of two");

    //allocate 1GB each time.
    uint64_t times = total_size/GB(1);
    uint64_t remain = total_size%GB(1);
    uint64_t k = GB(1)/chunk_data_size;
   
   SPDK_NOTICELOG("Try to allocate %uGB memory\n",total_size/GB(1));
    uint8_t* page_base[times+1];
    uint64_t i = 0;
    for(;i<times;i++){
        page_base[i] = spdk_dma_malloc(GB(1),0x1000, NULL);
        assert(page_base[i] && "memory allocation failed");
        SPDK_NOTICELOG("Allocated 1GB memory, times:%u/%u\n",i,times);
    }
    if(remain){
        page_base[times] = spdk_dma_malloc(remain,0x1000, NULL);
        assert(page_base[times] && "memory allocation failed");
        SPDK_NOTICELOG("Allocated remain memory, times:%u/%u\n",times,times);
    }

    //init all chunk memory descriptor
    for(i=0;i<nb_chunks;i++){
        struct chunk_mem* mem = (struct chunk_mem*)(g_mem_desc + i*mem_desc_size);
        mem->bitmap[0].length = g_chunkmgr_worker.nb_pages_per_chunk;
        mem->page_base = page_base[i/k] + chunk_data_size*(i%k);
    }
}

static struct chunk_mem *
_get_one_chunk_mem(void){
    struct chunk_mem* mem = NULL;
    if(g_cur<g_total_chunks-1){
        mem = (struct chunk_mem*)(g_mem_desc+g_cur*g_desc_size);
        g_cur++;
    }
    return mem;
}

static struct worker_context*
_get_worker_context_from_pmgr(struct pagechunk_mgr* pmgr){
    uint32_t i =0;

    for(;i<g_chunkmgr_worker.nb_business_workers;i++){
        if(g_chunkmgr_worker.wctx_array[i]->pmgr == pmgr){
            return g_chunkmgr_worker.wctx_array[i];
        }
    }
    //I will get one except the program bug.
    //return (struct worker_context*)((uint64_t)pmgr-sizeof(struct worker_context));
    assert(0);
    return NULL;
}

static void 
_chunkmgr_lease_one_chunk_mem(void *ctx){
    struct chunk_miss_callback *cb_obj = ctx;
    struct pagechunk_mgr *requestor_pmgr = cb_obj->requestor_pmgr;
    struct pagechunk_mgr *executor_pmgr = cb_obj->executor_pmgr;

    //I want a chunk memory, but the chunk manager tells me that I am not a busy worker
    //Of couse, it is a bug
    assert(requestor_pmgr!=executor_pmgr);

    struct chunk_mem* mem = pagechunk_evict_one_chunk(executor_pmgr);
    cb_obj->mem = mem;
    cb_obj->kverrno = mem ? -KV_ESUCCESS : -KV_ECACHE;

    //I get one chunk memory, but I am not the original requestor. So I should
    //send the chunk memory to the original requestor. 
    struct worker_context* requestor_wctx = _get_worker_context_from_pmgr(cb_obj->requestor_pmgr);
    spdk_thread_send_msg(requestor_wctx->thread,cb_obj->finish_cb,cb_obj);
}

static struct worker_context* _chunkmgr_evaluate_workload(void){
    //It is a very simple solution. I just choose the one with
    //maximal hit rate.
    int nb_workers = g_chunkmgr_worker.nb_business_workers;
    uint64_t total_chunks = g_chunkmgr_worker.nb_max_chunks;

    uint64_t min=101, max=0;
    int min_idx=0,max_idx=0;
    for(int i=0;i<nb_workers;i++){
        uint64_t misses = g_chunkmgr_worker.wctx_array[i]->pmgr->miss_times;
        uint64_t visits = g_chunkmgr_worker.wctx_array[i]->pmgr->visit_times;
        uint64_t hit_rate = visits ? 100*(visits-misses)/visits : 0;   
        if(hit_rate<min) {
            min = hit_rate;
            min_idx = i;
        }
        if(hit_rate>max){
            max = hit_rate;
            max_idx = i;
        }
    }

    if(max-min<=5){
        //I think they have basically the same hit rate. So reject it;
        return NULL;
    }
    return g_chunkmgr_worker.wctx_array[max_idx];
}

static void
_chunkmgr_worker_get_one_chunk_mem(void *ctx){
    struct chunk_miss_callback *cb_obj = ctx;
    struct worker_context* requestor_wctx = _get_worker_context_from_pmgr(cb_obj->requestor_pmgr);

    struct chunk_mem* mem = _get_one_chunk_mem();
    if(mem){
        cb_obj->mem = mem;
        cb_obj->executor_pmgr = NULL;
        cb_obj->kverrno = -KV_ESUCCESS;

        spdk_thread_send_msg(requestor_wctx->thread,cb_obj->finish_cb,ctx);
    }
    else{
        //SPDK_NOTICELOG("Chunk mem request incomming,p_reqs:%u, mkgr_pool:%u\n", requestor_wctx->kv_request_internal_pool->nb_frees,requestor_wctx->pmgr->kv_chunk_request_pool->nb_frees);

        //I have no any available memory. Just lease one;
        struct worker_context* executor_wctx = _chunkmgr_evaluate_workload();
        if(!executor_wctx || (executor_wctx==requestor_wctx)){
             //All workers are busy. Just tell the requestor to perform LRU eviction;
            cb_obj->mem = NULL;
            cb_obj->executor_pmgr = cb_obj->requestor_pmgr;
            spdk_thread_send_msg(requestor_wctx->thread,_chunkmgr_lease_one_chunk_mem,cb_obj);
        }
        else{
            //Request a chunk from other worker
            cb_obj->executor_pmgr = executor_wctx->pmgr;
            spdk_thread_send_msg(executor_wctx->thread,
                                  _chunkmgr_lease_one_chunk_mem,cb_obj);
        }
    }
}

void chunkmgr_request_one_aysnc(struct chunk_miss_callback *cb_obj){
    assert(g_chunkmgr_worker.thread!=NULL);
    spdk_thread_send_msg(cb_obj->requestor_pmgr->chunkmgr_worker->thread,
                         _chunkmgr_worker_get_one_chunk_mem, cb_obj);
}

// struct chunk_mem* chunkmgr_request_one(struct pagechunk_mgr* pmgr){
//     assert(g_chunkmgr_worker.thread!=NULL);

//     pthread_mutex_lock(&g_mutex);
//     struct chunk_mem* mem = _get_one_chunk_mem();
//     pthread_mutex_unlock(&g_mutex);

//     return mem;
// }

void chunkmgr_release_one(struct pagechunk_mgr* pmgr,struct chunk_mem* mem){
    assert(0 && "Not implemented");
}

struct chunkmgr_worker_context* 
chunkmgr_worker_alloc(struct chunkmgr_worker_init_opts *opts){
    assert(g_chunkmgr_worker.thread==NULL);

    g_chunkmgr_worker.nb_business_workers = opts->nb_business_workers;
    g_chunkmgr_worker.nb_max_chunks = opts->nb_max_cache_chunks;
    g_chunkmgr_worker.nb_pages_per_chunk = opts->nb_pages_per_chunk;
    g_chunkmgr_worker.wctx_array = opts->wctx_array;
    g_chunkmgr_worker.nb_used_chunks = 0;

    struct spdk_cpuset cpuset;
    spdk_cpuset_zero(&cpuset);
    spdk_cpuset_set_cpu(&cpuset,opts->core_id,true);
    //pthread_mutex_init(&g_mutex,NULL);

    g_chunkmgr_worker.thread = spdk_thread_create("chunkmgr",&cpuset);
    //g_chunkmgr_worker.thread = spdk_thread_create("chunkmgr",NULL);
    assert(g_chunkmgr_worker.thread!=NULL);

    _chunk_mem_init(g_chunkmgr_worker.nb_max_chunks);
    return &g_chunkmgr_worker;
}

static int
_chunkmgr_stat_report(void*ctx){
    int events = 0;
    int i = 0;
    int nb_workers = g_chunkmgr_worker.nb_business_workers;
    uint64_t total_chunks = g_chunkmgr_worker.nb_max_chunks;

    for(;i<nb_workers;i++){
        uint64_t chunks = g_chunkmgr_worker.wctx_array[i]->pmgr->nb_used_chunks;
        uint64_t misses = g_chunkmgr_worker.wctx_array[i]->pmgr->miss_times;
        uint64_t visits = g_chunkmgr_worker.wctx_array[i]->pmgr->visit_times;
        uint64_t hit_rate = visits ? 100*(visits-misses)/visits : 0;

        //clear stats per second
        g_chunkmgr_worker.wctx_array[i]->pmgr->miss_times = 1;
        g_chunkmgr_worker.wctx_array[i]->pmgr->visit_times  = 1;

        //SPDK_NOTICELOG("chunkmgr wid:%d, chunks:%lu, total_chunks:%lu, misses:%lu, visits:%lu, hit_rate:%lu\n",i,chunks,total_chunks,misses,visits,hit_rate);
    }
    return 0;
}

static void
_do_start(void*ctx){
    //Nothing should be done here.
    stat_poller = SPDK_POLLER_REGISTER(_chunkmgr_stat_report,NULL,1000000); //report every 1s
    SPDK_NOTICELOG("chunkmgr thread is working\n");
}

void chunkmgr_worker_start(void){
    assert(g_chunkmgr_worker.thread!=NULL);
    spdk_thread_send_msg(g_chunkmgr_worker.thread,_do_start,NULL);
}

static void
_do_destroy(void*ctx){
    spdk_thread_exit(g_chunkmgr_worker.thread);
    spdk_thread_destroy(g_chunkmgr_worker.thread);
    g_chunkmgr_worker.thread = NULL;

    SPDK_NOTICELOG("chunkmgr worker destroyed\n");
}

void chunkmgr_worker_destroy(void){
    assert(g_chunkmgr_worker.thread!=NULL);
    spdk_thread_send_msg(g_chunkmgr_worker.thread,_do_destroy,NULL);
}

