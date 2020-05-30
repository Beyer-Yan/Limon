#ifndef WORKLOAD_COMMON_H
#define WORKLOAD_COMMON_H 1
#include <stdint.h>
#include "random.h"
#include "item.h"

struct workload;
typedef enum available_bench {
   ycsb_a_uniform,
   ycsb_b_uniform,
   ycsb_c_uniform,
   ycsb_e_uniform,
   ycsb_a_zipfian,
   ycsb_b_zipfian,
   ycsb_c_zipfian,
   ycsb_e_zipfian,
   prod1,
   prod2,
} bench_t;

struct workload_api {
   int (*handles)(bench_t w); // do we handle that workload?
   void (*launch)(struct workload *w, bench_t b); // launch workload
   const char* (*name)(bench_t w); // pretty print the benchmark (e.g., "YCSB A - Uniform")
   const char* (*api_name)(void); // pretty print API name (YCSB or PRODUCTION)
   struct kv_item* (*create_unique_item)(uint64_t uid, uint64_t max_uid); // allocate an item in memory and return it
};
extern struct workload_api YCSB;
extern struct workload_api PRODUCTION;

struct workload {
   struct workload_api *api;
   int start_core;
   int nb_load_injectors;
   
   uint64_t nb_requests;
   uint64_t nb_items_in_db;
   uint64_t nb_requests_per_thread;
};

void repopulate_db(struct workload *w);
void run_workload(struct workload *w, bench_t bench);

struct kv_item *create_unique_item(uint64_t item_size, uint64_t uid);
struct workload_api *get_api(bench_t b);
#endif
