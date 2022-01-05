#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "common.h"
#include "item.h"
#include "kvutil.h"
#include "kvs.h"
#include "histogram.h"

static long udb_next(void) {
   long rand_key = 0;;
   long prob = uniform_next() % 100;
   if (prob < 95) {
      rand_key = 0 + zipf_next() % 182400000;
   } else {
      rand_key = 182400000 +  rand_key% (192000000-182400000);
   }
   return rand_key;
}


// Create a new item for the database
static struct kv_item *create_unique_item_udb(uint64_t uid, uint64_t max_uid) {
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
_udb_get_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item = ctx;
   if(kverrno){
      printf("Get error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   _update_stat(ori_item);
   free(ori_item);
}

static void
_udb_put_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item = ctx;
   if(kverrno){
      printf("Put error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   _update_stat(ori_item);
   free(ori_item);
}

static void
_udb_scan_get_complete(void*ctx, struct kv_item* item, int kverrno){
   uint64_t sid = (uint64_t)ctx;
   if(kverrno){
      printf("Scan get error %d for sid:%lu\n",kverrno,sid);
   }
}

static void launch_udb(struct workload *w, bench_t b,uint64_t* processed, int id) {
   uint64_t nb_requests = w->nb_requests_per_thread;

   for(uint64_t i = 0; i < nb_requests; i++) {
      struct kv_item* item = create_unique_item_udb(udb_next(), w->nb_items_in_db);

      // 58% write 40% read 2% scan
      long random = uniform_next() % 100;
      if(random < 58) {
         kv_put_async(item,_udb_put_complete,item);
      } else if(random < 98) {
         kv_get_async(item,_udb_get_complete,item);
      } else {
         uint32_t scan_length = uniform_next()%99+1;
         uint64_t sid_array[scan_length];
         uint64_t found = kv_scan(item,scan_length,sid_array);
         
         if(found){
            for(uint64_t i=0;i<found;i++){
               kv_get_with_sid_async(sid_array[i],_udb_scan_get_complete,NULL);
            }
         }
         free(item);
      }
      *processed = i;
   }
}

/* Pretty printing */
static const char *name_udb(bench_t w) {
   if(w==udb){
      return "UDB workload";
   }
   return "??";
}

static int handles_udb(bench_t w) {
   if(w==udb){
      return 1;
   }
   return 0;
}

static const char* api_name_udb(void) {
   return "UDB";
}

struct workload_api UDB = {
   .handles = handles_udb,
   .launch = launch_udb,
   .name = name_udb,
   .api_name = api_name_udb,
   .create_unique_item = create_unique_item_udb,
};
