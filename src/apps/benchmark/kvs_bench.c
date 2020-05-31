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
		.nb_items_in_db = 30000000LU,
		.nb_load_injectors = 4,
		.start_core = 10,
	};

	printf("Initializing random number generator (Zipf) -- this might take a while for large databases...\n");
    init_zipf_generator(0, w.nb_items_in_db - 1); 
	printf("Random number generator init completes\n");
	
	//Pre-fill the data into the database.
	repopulate_db(&w);
	   /* Launch benchs */
	bench_t workloads[] = {
		ycsb_a_uniform, ycsb_b_uniform, ycsb_c_uniform,
		ycsb_a_zipfian, ycsb_b_zipfian, ycsb_c_zipfian,
		//ycsb_e_uniform, ycsb_e_zipfian, // Scans
	};

	char* bench_name[] = {
		"ycsb_a_uniform","ycsb_b_uniform","ycsb_c_uniform",
		"ycsb_a_zipfian","ycsb_b_zipfian","ycsb_c_zipfian"
	};

	for(int i=0; i<sizeof(workloads)/sizeof(workloads[0]);i++){
		if(workloads[i] == ycsb_e_uniform || workloads[i] == ycsb_e_zipfian) {
			//requests for YCSB E are longer (scans) so we do less
			w.nb_requests = 2000000LU; 
		} else {
			w.nb_requests = 5000000LU;
		}
		printf("Benchmark starts, %s\n",bench_name[i]);
		run_workload(&w, workloads[i]);
	}
	printf("ALl workloads complete\n");
	kvs_shutdown();
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
    opts->devname = "Nvme0n1";
    opts->kvs_name = "kvs_bench";
    opts->max_cache_chunks = 30000;
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

	spdk_app_opts_init(&opts);

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
