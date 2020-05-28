#include <stdint.h>
#include <stdbool.h>
#include "kvs.h"

#include "spdk/env.h"
#include "spdk/event.h"

#include <pthread.h>

#include <stdatomic.h>
#include <time.h>

struct batch_context{
    int core_id;
    int start_num;
    int nb_items;
    int op;
    int vsize;
};

void pin_me_on(int core) {

   cpu_set_t cpuset;
   pthread_t thread = pthread_self();

   CPU_ZERO(&cpuset);
   CPU_SET(core, &cpuset);

   int s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
   if (s != 0){
       printf("pin failed\n");
       exit(-1);
   }
}

static void
_batch_op_complete(void*ctx, struct kv_item* item,  int kverrno){
    if(kverrno){
        printf("Op error\n");
        exit(-1);
    }
    static atomic_int i = 0;
    int cnt = atomic_fetch_add(&i,1);
    if(cnt%100000==0){
        printf("Op success, count:%d\n",cnt);
    }
    free(ctx);
}

static void
_batch_test(struct batch_context *bctx){
    int end_item = bctx->start_num + bctx->nb_items;
    int start_num = bctx->start_num;
    int op = bctx->op;

    static char* op_name[3] = {"Put","Get","Updata"};

    struct timeval t0,t1;
    gettimeofday(&t0,NULL);

    for(;start_num<end_item;start_num++){
        struct kv_item *item = malloc(sizeof(struct item_meta) + 4 + bctx->vsize);
        memcpy(item->data,&start_num,4);
        item->meta.ksize = 4;
        item->meta.vsize = bctx->vsize;
        switch(op){
            case 0:
                kv_put_async(item,_batch_op_complete,item);
                break;
            case 1: 
                kv_get_async(item,_batch_op_complete,item);  
                break;
            case 2:
                kv_put_async(item,_batch_op_complete,item);
                break;
            default:
                break;
        }
    }
    gettimeofday(&t1,NULL);
    double secs = ((t1.tv_sec*1000000+t1.tv_usec)- (t0.tv_sec*1000000+t0.tv_usec))/1000000;
    double pps = bctx->nb_items/secs;
    printf("%s test completes,w:%d, sec:%f, items:%d,pps:%f\n",op_name[op],bctx->core_id,secs,bctx->nb_items,pps);
}

static void*
_batch_test_start(void* ctx){
    struct batch_context *bctx = ctx;

    int core_id = bctx->core_id;
    int end_item = bctx->start_num + bctx->nb_items;
    int start_num = bctx->start_num;

    pin_me_on(core_id);
    printf("start id %d\n",bctx->core_id);

    //Test put
    printf("Testing add\n");
    bctx->op = 0;
    _batch_test(bctx);

    //Test get
    printf("Testing get\n");
    bctx->op = 1;
    _batch_test(bctx);

    //Test updata in place
    printf("Testing update in place\n");
    bctx->op = 2;
    _batch_test(bctx);

    //Test updata slab changed
    printf("Testing update slab changed\n");
    bctx->op = 2;
    bctx->vsize = 2000;
    _batch_test(bctx);

    
    return NULL;
}

static void
_start_batch_test(int start_core_id, int nb_workers, int nb_items_per_worker){
    pthread_t pid;
    struct batch_context *ctx ;
    
    for(int i=0;i<nb_workers;i++){
        ctx = malloc(sizeof(struct batch_context));
        ctx->core_id = start_core_id + i;
        ctx->start_num = i*nb_items_per_worker;
        ctx->nb_items = nb_items_per_worker;
        pthread_create(&pid,NULL,_batch_test_start,ctx);
    }
}

static void
hello_start(void*ctx, int kverrno){
    printf("Hello pemon~\n");
    _start_batch_test(10,1,300000);
}

static void
_kvs_opts_init(struct kvs_start_opts *opts){
    opts->devname = "Nvme0n1";
    opts->kvs_name = "hello_pemon";
    opts->max_cache_chunks = 1024;
    opts->max_io_pending_queue_size_per_worker = 64;
    opts->max_request_queue_size_per_worker = 128;
    opts->nb_works = 1;
    opts->reclaim_batch_size = 16;
    opts->reclaim_percentage_threshold = 80;
    opts->startup_fn = hello_start;
    opts->startup_ctx = NULL;
}

int
main(int argc, char **argv){
    struct spdk_app_opts opts = {0};
	int rc = 0;

	spdk_app_opts_init(&opts);

	opts.name = "hello_pemon";
	if ((rc = spdk_app_parse_args(argc, argv, &opts, NULL, NULL,
				      NULL, NULL)) !=
	    SPDK_APP_PARSE_ARGS_SUCCESS) {
		exit(rc);
	}

    struct kvs_start_opts kvs_opts;
    _kvs_opts_init(&kvs_opts);

    kvs_opts.spdk_opts = &opts;

    kvs_start_loop(&kvs_opts);

    //Shall not be here.
    printf("KVS %s stoped\n",opts.name);
    
	return -1;
}
