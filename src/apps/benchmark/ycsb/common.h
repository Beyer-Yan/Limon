#ifndef WORKLOAD_COMMON_H
#define WORKLOAD_COMMON_H 1

#include <stdint.h>
#include "random.h"
#include "item.h"

typedef enum available_bench {
   ycsb_a_uniform = 0,
   ycsb_b_uniform,
   ycsb_c_uniform,
   ycsb_d_uniform,
   ycsb_e_uniform,
   ycsb_f_uniform,
   ycsb_a_zipfian,
   ycsb_b_zipfian,
   ycsb_c_zipfian,
   ycsb_d_zipfian,
   ycsb_e_zipfian,
   ycsb_f_zipfian,
   udb,
   etc,
} bench_t;

struct workload {
   struct workload_api *api;
   int start_core;
   int nb_load_injectors;
   
   uint64_t nb_requests;
   uint64_t nb_items_in_db;
   uint64_t nb_requests_per_thread;
};

struct workload_api {
   // do we handle that workload?
   int (*handles)(bench_t w); 

   // launch workload
   void (*launch)(struct workload *w, bench_t b, uint64_t* processed, int wid); 

   // pretty print the benchmark (e.g., "YCSB A - Uniform")
   const char* (*name)(bench_t w);

   // pretty print API name (YCSB or PRODUCTION)
   const char* (*api_name)(void); 

   void (*print_histogram)(void);

   // allocate an item in memory and return it
   struct kv_item* (*create_unique_item)(uint64_t uid, uint64_t max_uid); 
};
extern struct workload_api YCSB;
extern struct workload_api UDB;
extern struct workload_api ETC;


void repopulate_db(struct workload *w);
void run_workload(struct workload *w, bench_t bench);

struct kv_item *create_unique_item(uint64_t item_size, uint64_t uid);
struct kv_item* create_item_from_item(struct kv_item *item);

struct workload_api *get_api(bench_t b);

#endif
