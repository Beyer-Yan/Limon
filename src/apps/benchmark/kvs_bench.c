#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include "kvs.h"
#include "slab.h"
#include "kvutil.h"

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/blob_bdev.h"
#include "spdk/blob.h"
#include "spdk/log.h"
#include "spdk/string.h"

#include "ycsb/common.h"
#include "ycsb/histogram.h"

static const char *_g_kvs_getopt_string = "W:D:Q:C:I:N"; 

static struct option _g_app_long_cmdline_options[] = {
#define WORKERS_OPT_IDX         'W'
    {"workers",required_argument,NULL,WORKERS_OPT_IDX},
#define DEVNAME_OPT_IDX         'D'
    {"devname",required_argument,NULL,DEVNAME_OPT_IDX},
#define QUEUE_DEPTH_OPT_IDX     'Q'
    {"queue-depth",required_argument,NULL,QUEUE_DEPTH_OPT_IDX},
#define CHUNKS_IDX              'C'
    {"chunks",required_argument,NULL,CHUNKS_IDX},
#define INJECTORS_OPT_IDX       'I'
    {"injectors",required_argument,NULL,INJECTORS_OPT_IDX},
#define ITMES_OPT_IDX           'N'
    {"items",required_argument,NULL,ITMES_OPT_IDX}
};


static void
_bench_usage(void){
	printf(" -K, --max-key-length <num>   the max key length of the current kvs(default:%u)\n",
                                          _g_default_opts.max_key_length);
	printf(" -S, --shards <num>           the number of shards(default:%u)\n",
                                          _g_default_opts.nb_shards);
    printf(" -C, --chunks-per-node <num>  the chunks per reclaim node(default:%u)\n",
                                          _g_default_opts.nb_chunks_per_reclaim_node);
    printf(" -f, --force-format           format the kvs forcely\n");
    printf(" -D, --devname <namestr>      the devname(default:%s)\n",
                                          _g_default_opts.devname);
    printf(" -N, --init-nodes  <num>      the init nodes for each slab(default:%u)\n",
                                          _g_default_opts.nb_init_nodes_per_slab);
    printf(" -E, --dump                   Dump the existing kvs format\n");
}

static int
_kvs_parse_arg(int ch, char *arg){
	return 0;
}

static void
_kvs_usage(void){

}

static void*
_do_start_benchmark(void*ctx){

	struct workload w = {
		.api = &YCSB,
		.nb_items_in_db = 40000000LU,
		.nb_load_injectors = 4,
		.start_core = 20,
	};

	printf("Initializing random number generator (Zipf) -- this might take a while for large databases...\n");
    init_zipf_generator(0, w.nb_items_in_db - 1); 
	printf("Random number generator init completes\n");
	
	//Pre-fill the data into the database.
	repopulate_db(&w);
	   /* Launch benchs */
	bench_t workloads[] = {
		//ycsb_f_uniform
		//ycsb_a_uniform
		ycsb_a_uniform, ycsb_b_uniform, ycsb_c_uniform,ycsb_d_uniform,ycsb_f_uniform,
		//ycsb_a_zipfian, ycsb_b_zipfian, ycsb_c_zipfian,ycsb_d_zipfian,ycsb_f_zipfian,
		ycsb_e_uniform, 
		//ycsb_e_zipfian, // Scans
	};

	histogram_init();

	for(int i=0; i<sizeof(workloads)/sizeof(workloads[0]);i++){
		if(workloads[i] == ycsb_e_uniform || workloads[i] == ycsb_e_zipfian) {
			//requests for YCSB E are longer (scans) so we do less
			w.nb_requests = 1000000LU; 
		} else {
			w.nb_requests = 10000000LU;
		}
		printf("Benchmark starts, %s\n",w.api->name(workloads[i]));
		histogram_reset();
		run_workload(&w, workloads[i]);
		histogram_print();
	}
	printf("All workloads complete, ctrl+c to stop the program\n");
}

static void
_kvs_bench_start(void*ctx,int kverrno){
	if(kverrno){
		printf("Startup error\n");
		kvs_shutdown();
		return;
	}

	//Create a thread to prevent blobcking the master core.
	pthread_t *thread = malloc(sizeof(pthread_t));
	assert(thread!=NULL);
	pthread_create(thread,NULL,_do_start_benchmark,thread);
}

static void
_kvs_opts_init(struct kvs_start_opts *opts){
    opts->devname = "Raid0";
    opts->kvs_name = "kvs_bench";
    opts->max_cache_chunks = 3000;
    opts->max_io_pending_queue_size_per_worker = 64;
    opts->max_request_queue_size_per_worker = 128;
    opts->nb_works = 4;
    opts->reclaim_batch_size = 16;
    opts->reclaim_percentage_threshold = 80;
    opts->startup_fn = _kvs_bench_start;
    opts->startup_ctx = NULL;
}

int
main(int argc, char **argv){
    struct spdk_app_opts opts = {};
	int rc = 0;

	spdk_app_opts_init(&opts,sizeof(opts));

	opts.name = "kvs_bench";
	if ((rc = spdk_app_parse_args(argc, argv, &opts, NULL, NULL,
				      _kvs_parse_arg, _kvs_usage)) !=
	    SPDK_APP_PARSE_ARGS_SUCCESS) {
		exit(rc);
	}

    struct kvs_start_opts kvs_opts;
    _kvs_opts_init(&kvs_opts);

    kvs_opts.spdk_opts = &opts;

    kvs_start_loop(&kvs_opts);

	return rc;
}
