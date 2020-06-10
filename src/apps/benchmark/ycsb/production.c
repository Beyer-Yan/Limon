#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "common.h"
#include "item.h"
#include "kvutil.h"
#include "kvs.h"

// Create a new item for the database
static struct kv_item *create_unique_item_prod(uint64_t uid, uint64_t max_uid) {
   uint64_t item_size;
   if(uid*100LU/max_uid < 1) // 1%
      item_size = 100;
   else if(uid*100LU/max_uid < 82) // 81% + 1%
      item_size = 400;
   else if(uid*100LU/max_uid < 98)
      item_size = 1024;
   else
      item_size = 4096;

   return create_unique_item(item_size, uid);
}

static inline void
_update_stat(struct kv_item* item){
   uint64_t tsc0,tsc1;
   memcpy(&tsc0,item->meta.cdt,sizeof(tsc0));
   rdtscll(tsc1);
   uint64_t tsc_diff = tsc1 - tsc0;
   histogram_update(tsc_diff);
}

static void
_prod_get_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item = ctx;
   if(kverrno){
      printf("Get error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   _update_stat(ori_item);
   free(ori_item);
}

static void
_prod_put_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item = ctx;
   if(kverrno){
      printf("Put error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   _update_stat(ori_item);
   free(ori_item);
}

static void launch_prod(struct workload *w, bench_t b, int id) {
   declare_periodic_count;
   random_gen_t rand_next = (b==prod1)?(production_random1):(production_random2);
   uint64_t nb_requests = w->nb_requests_per_thread;
   struct kv_iterator *it = kv_iterator_alloc();

   for(uint64_t i = 0; i < nb_requests; i++) {
      struct kv_item* item = create_unique_item_prod(rand_next(), w->nb_items_in_db);

      // 58% write 40% read 2% scan
      long random = uniform_next() % 100;
      if(random < 58) {
         kv_put_async(item,_prod_put_complete,item);
      } else if(random < 98) {
         kv_get_async(item,_prod_get_complete,item);
      } else {

         struct kv_item* item = create_unique_item_prod(rand_next(),w->nb_items_in_db);
         uint32_t scan_length = uniform_next()%99+1;

         if(!kv_iterator_seek(it,item)){
            printf("Error in seek item, key:%lu\n",*(uint64_t*)item->data);
            exit(-1);
         }

         for(uint64_t i = 0; i < scan_length; i++) {
            if(kv_iterator_next(it)){
               item = create_item_from_item(kv_iterator_item(it));
               kv_get_async(item, _prod_get_complete, (void*)i);
            }
         }
      }
      periodic_count(1000, "Production Load Injector:%02d, (%lu%%)", id ,i*100UL/nb_requests);
   }
}

/* Pretty printing */
static const char *name_prod(bench_t w) {
   switch(w) {
      case prod1:
         return "Production 1";
      case ycsb_b_uniform:
         return "Production 2";
      default:
         return "??";
   }
}

static int handles_prod(bench_t w) {
   switch(w) {
      case prod1:
      case prod2:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_prod(void) {
   return "PRODUCTION";
}

struct workload_api PRODUCTION = {
   .handles = handles_prod,
   .launch = launch_prod,
   .name = name_prod,
   .api_name = api_name_prod,
   .create_unique_item = create_unique_item_prod,
};
