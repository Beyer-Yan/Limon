#include <stdint.h>
#include <stdbool.h>
#include "kvs.h"

#include "spdk/env.h"
#include "spdk/event.h"

#include <pthread.h>

#include <stdatomic.h>

struct batch_context{
    int core_id;
    int start_num;
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
_batch_get_complete(void*ctx, struct kv_item* item,  int kverrno){
    if(kverrno){
        printf("Get error\n");
        exit(-1);
    }
    if(memcmp("testb",item->data+4,5)){
        printf("Get value mismatch, get_val:%5s\n",item->data+4 );

        exit(-1);
    }
    static atomic_int i = 0;
    int cnt = atomic_fetch_add(&i,1);
    if(cnt%10000==0){
        printf("Gut key success, count:%d\n",cnt);
    }
}

static void
_batch_read_test(void){
    printf("Testing get\n");
    int i = 0;
    for(;i<100000;i++){
        struct kv_item *item = malloc(sizeof(struct item_meta) + 4 + 5);
        memcpy(item->data,&i,4);
        item->meta.ksize = 4;
        kv_get_async(item,_batch_get_complete,item);
    }
    printf("Gut test completes\n");
    exit(-1);
}

static void
_batch_put_complete(void*ctx, struct kv_item* item,  int kverrno){
    if(kverrno){
        printf("Put error\n");
        exit(-1);
    }
    static atomic_int i = 0;
    int cnt = atomic_fetch_add(&i,1);
    if(cnt%10000==0){
        printf("Put key success, count:%d\n",cnt);
    }
}

static void*
_batch_put_test(void* ctx){
    struct batch_context *bctx = ctx;
    int nb = bctx->start_num + 1000000;
    pin_me_on(bctx->core_id);
    printf("start id %d\n",bctx->core_id);
    int i = bctx->start_num;
    for(;i<nb;i++){
        struct kv_item *item = malloc(sizeof(struct item_meta) + 4 + 5);
        memcpy(item->data,&i,4);
        memcpy(item->data+4,"testb",5);
        item->meta.ksize = 4;
        item->meta.vsize = 5;
        kv_put_async(item,_batch_put_complete,item);
    }
    printf("Put test completes\n");
    exit(0);
    //_batch_read_test();
    return NULL;
}

static void
_start_batch_test(void){
    pthread_t pid;
    struct batch_context *ctx = malloc(sizeof(struct batch_context));
    ctx->core_id = 10;
    ctx->start_num = 1000000;
    
    pthread_create(&pid,NULL,_batch_put_test,ctx);

    ctx = malloc(sizeof(struct batch_context));
    ctx->core_id = 11;
    ctx->start_num = 2000000;
    pthread_create(&pid,NULL,_batch_put_test,ctx);
}

static void
_get_complete(void*ctx, struct kv_item* item,  int kverrno){
    struct kv_item *origin_item = ctx;
    if(!kverrno){
        printf("Get sucess\n");
    }
    if(memcmp(origin_item->data,item->data,10)){
        printf("Error:item mismatch!!\n");
    }
    _start_batch_test();
}

static void
_put_complete(void*ctx, struct kv_item* item, int kverrno){
    struct kv_item *origin_item = ctx;
    if(!kverrno){
        printf("Put sucess\n");
    }
    kv_get_async(origin_item,_get_complete,origin_item);
}

static void
hello_start(void*ctx, int kverrno){
    printf("Hello pemon~\n");
    //struct kv_item *item = malloc(sizeof(struct item_meta) + 5 + 5);
    //item->meta.ksize = 5;
    //item->meta.vsize = 5;
    //memcpy(item->data,"12345",5);
    //memcpy(item->data + 5,"54321",5);
    //kv_put_async(item,_put_complete,item);
    _start_batch_test();
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
    printf("Kvs start failed\n");
    
	return -1;
}
