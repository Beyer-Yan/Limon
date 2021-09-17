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

static const char *_g_kvs_getopt_string = "D:S:I:Q:C:N:T:"; 

static struct option _g_app_long_cmdline_options[] = {
#define DEVNAME_OPT_IDX         'D'
    {"devname",required_argument,NULL,DEVNAME_OPT_IDX},
#define WORKERS_OPT_IDX         'S'
    {"workers",required_argument,NULL,WORKERS_OPT_IDX},
#define INJECTORS_OPT_IDX       'I'
    {"injectors",required_argument,NULL,INJECTORS_OPT_IDX},
#define QUEUE_DEPTH_OPT_IDX     'Q'
    {"queue-depth",required_argument,NULL,QUEUE_DEPTH_OPT_IDX},
#define CACHES_IDX              'C'
    {"caches",required_argument,NULL,CACHES_IDX},
#define ITMES_OPT_IDX           'N'
    {"items",required_argument,NULL,ITMES_OPT_IDX},
#define IO_OPT_IDX              'T'
    {"io_cycle_us",required_argument,NULL,IO_OPT_IDX}
};

struct kvs_bench_opts{
    char* devname;
	char* bench_name;

	uint32_t nb_workers;
	uint32_t nb_injectors;

	uint32_t queue_size;
	uint32_t caches; //GB
	uint64_t nb_items;

    uint64_t io_cyle_us; 
};

static struct kvs_bench_opts _g_default_opts = {
	.devname = "Nvme0n1",
	.bench_name = "kvs_ycsb",
	.nb_workers = 1,
	.nb_injectors = 2,
	.queue_size = 16,
	.caches = 5, /* 10GB */
	.nb_items = 26000000,
    .io_cyle_us = 0
};

static void
_bench_usage(void){
	printf(" -D, --devname <namestr>      block devname(default:%s)\n",
                                          _g_default_opts.devname);
	printf(" -S, --workers <num>          number of workers(default:%u)\n",
                                          _g_default_opts.nb_workers);
    printf(" -I, --injectors <num>        number of injector(default:%u)\n",
                                          _g_default_opts.nb_injectors);
    printf(" -Q, --queue-size <num>       queue size(default:%u)\n",
										  _g_default_opts.queue_size);
    printf(" -C, --caches <num>           number of cache chunks(default:%uGB)\n",
                                          _g_default_opts.caches);
    printf(" -N, --items  <numGB>         total items in db(default:%lu)\n",
                                          _g_default_opts.nb_items);
    printf(" -T, --io-cycle <num>         io polling cycle(default:%lu)\n",
                                          _g_default_opts.io_cyle_us);
}

static int
_bench_parse_arg(int ch, char *arg){
	switch(ch){
        case 'D':{
            _g_default_opts.devname = arg;
            break;
        }
        case 'S':{
            long workers  = spdk_strtol(arg,0);
            if(workers>0){
                _g_default_opts.nb_workers = workers;
            }
            else{
                fprintf(stderr,"The workers shall be a positive integer number\n");
                return -EINVAL;
            }
            break;
        }
        case 'I':{
            long injectors = spdk_strtol(arg,0);
            if(injectors>0){
                _g_default_opts.nb_injectors = injectors;
            }
            else{
                fprintf(stderr,"The injectors shall be a positive number\n");
                return -EINVAL;
            }
            break;
        }
        case 'Q':{
            long queue_size = spdk_strtol(arg,0);
            if(queue_size>0){
                _g_default_opts.queue_size = queue_size;
            }
            else{
                fprintf(stderr,"The queue size shall be a positive number\n");
                return -EINVAL;
            }
            break;
        }
		case 'C':{
            long caches = spdk_strtol(arg,0);
            if(caches>0){
                _g_default_opts.caches = caches;
            }
            else{
                fprintf(stderr,"The cache chunks shall be a positive number\n");
                return -EINVAL;
            }
            break;
        }
		case 'N':{
            long items = spdk_strtol(arg,0);
            if(items>0){
                _g_default_opts.nb_items = items;
            }
            else{
                fprintf(stderr,"The db items shall be a positive number\n");
                return -EINVAL;
            }
            break;
        }
		case 'T':{
            long cycle = spdk_strtol(arg,0);
            if(cycle>=0){
                _g_default_opts.io_cyle_us = cycle;
            }
            else{
                fprintf(stderr,"The io_cyle_us shall be a positive number\n");
                return -EINVAL;
            }
            break;
        }
        default:
            return -EINVAL;
            break;
    }
    return 0;
}

static void*
_do_start_benchmark(void*ctx){

	struct workload w = {
		.api = &YCSB,
		.nb_items_in_db = _g_default_opts.nb_items,
		.nb_load_injectors = _g_default_opts.nb_injectors,
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
        //ycsb_a_uniform,ycsb_e_uniform
        //ycsb_a_zipfian,
        ycsb_c_uniform,
        //ycsb_c_zipfian,
        //ycsb_c_zipfian
		//ycsb_a_uniform,ycsb_c_uniform,ycsb_e_uniform,
        //ycsb_a_zipfian,ycsb_c_zipfian,ycsb_e_zipfian,
        //ycsb_d_uniform
        //ycsb_e_uniform,ycsb_f_uniform,
        //ycsb_a_uniform,ycsb_c_uniform,ycsb_e_uniform,
        //ycsb_a_zipfian,ycsb_c_zipfian,ycsb_e_zipfian,
		//ycsb_a_uniform, ycsb_b_uniform, ycsb_c_uniform,ycsb_d_uniform,ycsb_e_uniform,ycsb_f_uniform,
        //ycsb_a_zipfian, ycsb_b_zipfian, ycsb_c_zipfian,ycsb_d_zipfian,ycsb_e_zipfian,ycsb_f_zipfian
        //ycsb_c_zipfian,
        //ycsb_f_uniform,
		//ycsb_a_zipfian, ycsb_b_zipfian, ycsb_c_zipfian,ycsb_d_zipfian,ycsb_f_zipfian,
		//ycsb_e_zipfian, // Scans
        //ycsb_c_zipfian
	};

	histogram_init();

	for(uint32_t i=0; i<sizeof(workloads)/sizeof(workloads[0]);i++){
		//30% extra requests for warm-up
		if(workloads[i] == ycsb_e_uniform || workloads[i] == ycsb_e_zipfian) {
			//requests for YCSB E are longer (scans) so we do less
			w.nb_requests = 3600000LU; 
		} else {
			w.nb_requests = 13000000LU;
		}
		histogram_reset();
        uint64_t start = spdk_get_ticks();
		run_workload(&w, workloads[i]);
        uint32_t us = kv_cycles_to_us(spdk_get_ticks()-start);
        uint64_t qps = (uint64_t)((double)w.nb_requests/((double)us/1000000.0));
        printf("Workload %s, requests (%lu/s)\n",w.api->name(workloads[i]),qps);
		histogram_print();
	}
	printf("All workloads complete, ctrl+c to stop the program\n");
    //wait 1 second for left requests.
    spdk_delay_us(1000000);
    kvs_shutdown();
	return NULL;
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
    //pthread_join(thread,NULL);
}

static void
_kvs_opts_init(struct kvs_start_opts *opts){
    opts->devname = _g_default_opts.devname;
    opts->kvs_name = _g_default_opts.bench_name;
    
    uint64_t pages_per_GB = 1024u*1024u*1024u/KVS_PAGE_SIZE;
    opts->max_cache_pages = _g_default_opts.caches*pages_per_GB;

    opts->max_request_queue_size_per_worker = _g_default_opts.queue_size;
    opts->nb_works = _g_default_opts.nb_workers;
    opts->reclaim_batch_size = 4;
    opts->reclaim_percentage_threshold = 80;
    opts->io_cycle = _g_default_opts.io_cyle_us;
    opts->startup_fn = _kvs_bench_start;
    opts->startup_ctx = NULL;
}

static void
_kvs_print_parameters(void){
    printf("block devname     \t:%s\n",_g_default_opts.devname);
    printf("number of workers \t:%u\n",_g_default_opts.nb_workers);
    printf("number of injectors \t:%u\n",_g_default_opts.nb_injectors);
    printf("queue size        \t:%u\n",_g_default_opts.queue_size);
    printf("cache size (GB)   \t:%u\n",_g_default_opts.caches);
    printf("total items in db \t:%lu\n",_g_default_opts.nb_items);	
    printf("io cycle \t:%lu\n",_g_default_opts.io_cyle_us);	
}

int
main(int argc, char **argv){
    struct spdk_app_opts opts = {};
	int rc = 0;

	spdk_app_opts_init(&opts,sizeof(opts));

	opts.name = _g_default_opts.bench_name;
	if ((rc = spdk_app_parse_args(argc, argv, &opts, 
								  _g_kvs_getopt_string, 
								  _g_app_long_cmdline_options,
								  _bench_parse_arg,
								  _bench_usage)) !=
	    SPDK_APP_PARSE_ARGS_SUCCESS) {
		exit(rc);
	}

    struct kvs_start_opts kvs_opts;
    _kvs_opts_init(&kvs_opts);

    kvs_opts.spdk_opts = &opts;

	_kvs_print_parameters();
    kvs_start_loop(&kvs_opts);

	return rc;
}
