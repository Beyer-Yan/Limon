
#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/log.h"
#include "pagechunk.h"
#include "worker_internal.h"
#include "kverrno.h"
#include "slab.h"
#include "pool.h"
#include "kvutil.h"

struct chunkmgr_worker_context{
    uint32_t nb_business_workers;
    struct worker_context **wctx_array;

    uint32_t nb_max_chunks;
    uint32_t nb_used_chunks;
    uint32_t nb_pages_per_chunk;

    struct spdk_thread *thread;
};

//Sinleton mode
struct chunkmgr_worker_context g_chunkmgr_worker;

static struct object_cache_pool  *_g_mem_pool;

//Allocate chunk memory in one-shot mode.
static void
_chunk_mem_init(uint64_t nb_chunks){

    uint32_t chunk_size = g_chunkmgr_worker.nb_pages_per_chunk * PAGE_SIZE;
    uint32_t bitmap_data_size = g_chunkmgr_worker.nb_pages_per_chunk/8 + 1;
    uint32_t header_size = KV_ALIGN(sizeof(struct chunk_mem) + sizeof(struct bitmap) + bitmap_data_size,0x1000);
    uint32_t chunk_mem_size = header_size + chunk_size;

    uint64_t pool_size = KV_ALIGN(pool_header_size(nb_chunks),0x1000);

    uint8_t* data = spdk_malloc(pool_size + chunk_mem_size*nb_chunks,
						0x1000, NULL, SPDK_ENV_LCORE_ID_ANY,
						SPDK_MALLOC_DMA);
    assert(data!=NULL);
    
    _g_mem_pool = (struct object_cache_pool*)data;
    data += pool_size;
    pool_header_init(_g_mem_pool,nb_chunks,chunk_mem_size,header_size,data);
    
    int i = 0;
    for(;i<nb_chunks;i++){
        struct chunk_mem* mem = (struct chunk_mem*)(data + chunk_mem_size*i);
        mem->bitmap = (struct bitmap*)(mem+1);
        mem->bitmap->length = g_chunkmgr_worker.nb_pages_per_chunk;
        mem->bitmap->data = (uint8_t*)(mem->bitmap + 1);
        mem->nb_bytes = chunk_mem_size;
        mem->data = (uint8_t*)(mem) + header_size;
    }
}

static struct chunk_mem *
_get_one_chunk_mem(void){
    struct chunk_mem* mem = pool_get(_g_mem_pool);
    if(!mem){
        return NULL;
    }
    return mem;
}

static void
_release_one_chunk_mem(struct chunk_mem *mem){
    assert(mem!=NULL);
    pool_release(_g_mem_pool,mem);
}

struct worker_context* _chunkmgr_evaluate_worklaod(void){
    //@todo
    return NULL;
}

static struct worker_context*
_get_worker_context_from_pmgr(struct pagechunk_mgr* pmgr){
    int i =0;

    for(;i<g_chunkmgr_worker.nb_business_workers;i++){
        if(g_chunkmgr_worker.wctx_array[i]->pmgr == pmgr){
            return g_chunkmgr_worker.wctx_array[i];
        }
    }
    //I will get one except the program bug.
    //return (struct worker_context*)((uint64_t)pmgr-sizeof(struct worker_context));
    assert(0);
}

static void 
_chunkmgr_lease_one_chunk_mem(void *ctx){
    struct chunk_miss_callback *cb_obj = ctx;
    struct pahechunk_mgr *requestor_pmgr = cb_obj->requestor_pmgr;
    struct pahechunk_mgr *executor_pmgr = cb_obj->executor_pmgr;
    struct chunk_mem* mem = pagechunk_evict_one_chunk(executor_pmgr);
    
    if(!mem){
        cb_obj->mem = NULL;
        cb_obj->kverrno = -KV_ECACHE;
    }
    else{
        cb_obj->mem = mem;
        cb_obj->kverrno = -KV_ESUCCESS;
    }

    if(requestor_pmgr==executor_pmgr){
        //I want a chunk memory, but the chunk manager tells me that I am not a busy worker
        //and I should perform LRU from myself.
        cb_obj->finish_cb(cb_obj);
    }
    else{
        //I get one chunk memory, but I am not the original requestor. So I should
        //send the chunk memory to the original requestor. 
        struct worker_context* requestor_wctx = _get_worker_context_from_pmgr(cb_obj->requestor_pmgr);
        pagechunk_send_message(requestor_wctx->thread,cb_obj->finish_cb,cb_obj);
    }
}

static void
_chunkmgr_worker_get_one_chunk_mem(void *ctx){
    struct chunk_miss_callback *cb_obj = ctx;
    struct worker_context* requestor_wctx = _get_worker_context_from_pmgr(cb_obj->requestor_pmgr);

    struct chunk_mem* mem = _get_one_chunk_mem();
    if(!mem){
        cb_obj->mem = mem;
        cb_obj->executor_pmgr = NULL;
        cb_obj->kverrno = -KV_ESUCCESS;

        spdk_thread_send_msg(requestor_wctx->thread,cb_obj->finish_cb,ctx);
    }
    else{
        //I have no any available memory. Just lease one;
         struct worker_context* executor_wctx = _chunkmgr_evaluate_worklaod();
         if(!executor_wctx){
             //All workers are busy. Just tell the requestor to perform LRU eviction;
             cb_obj->mem = NULL;
             spdk_thread_send_msg(requestor_wctx->thread,_chunkmgr_lease_one_chunk_mem,cb_obj);
         }
         else{
             cb_obj->executor_pmgr = executor_wctx->pmgr;
             spdk_thread_send_msg(executor_wctx->pmgr,_chunkmgr_lease_one_chunk_mem,cb_obj);
         }
    }
}

void chunkmgr_request_one_aysnc(struct chunk_miss_callback *cb_obj){
    spdk_thread_send_msg(cb_obj->requestor_pmgr->chunkmgr_worker->thread,
                         _chunkmgr_worker_get_one_chunk_mem, cb_obj);
}

static void
_chunkmgr_worker_release_one_chunk_mem(void *ctx){
    struct chunk_mem* mem = ctx;
    _release_one_chunk_mem(mem);
}

void chunkmgr_release_one(struct pagechunk_mgr* pmgr,struct chunk_mem* mem){
    spdk_thread_send_msg(pmgr->chunkmgr_worker->thread, _chunkmgr_worker_release_one_chunk_mem, mem);
}

struct chunkmgr_worker_context* 
chunkmgr_worker_init(struct chunkmgr_worker_init_opts *opts){
    g_chunkmgr_worker.nb_business_workers = opts->nb_business_workers;
    g_chunkmgr_worker.nb_max_chunks = opts->nb_max_cache_chunks;
    g_chunkmgr_worker.nb_pages_per_chunk = opts->nb_pages_per_chunk;
    g_chunkmgr_worker.wctx_array = opts->wctx_array;
    g_chunkmgr_worker.nb_used_chunks = 0;

    struct spdk_cpuset cpuset;
    spdk_cpuset_zero(&cpuset);
    spdk_cpuset_set_cpu(&cpuset,opts->core_id,true);

    g_chunkmgr_worker.thread = spdk_thread_create("chunkmgr",&cpuset);
    assert(g_chunkmgr_worker.thread!=NULL);

    _chunk_mem_init(g_chunkmgr_worker.nb_max_chunks);
    return &g_chunkmgr_worker;
}

static void
_do_start(void*ctx){
    //Nothing should be done here.
    SPDK_NOTICELOG("chunkmgr thread is working\n");
}

void chunkmgr_worker_start(){
    assert(g_chunkmgr_worker.thread!=NULL);
    spdk_thread_send_msg(g_chunkmgr_worker.thread,_do_start,NULL);
}

