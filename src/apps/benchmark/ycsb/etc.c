#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "common.h"
#include "item.h"
#include "kvutil.h"
#include "kvs.h"
#include "histogram.h"

static long etc_next(struct workload *w) {
   long rand_key = 0;;
   long prob = uniform_next() % 100;
   uint64_t div = w->nb_items_in_db*95/100;
   if (prob < 95) {
      //rand_key = 0 + zipf_next() % 182400000;
      rand_key = 0 + zipf_next() % div;
   } else {
      //rand_key = 182400000 +  rand_key% (192000000-182400000);
      rand_key = div +  rand_key% (w->nb_items_in_db-div);
   }
   return rand_key;
}

// Create a new item for the database
static struct kv_item *create_unique_item_etc(uint64_t uid, uint64_t max_uid) {
   uint64_t value_size = uniform_next();
   if(uid*100LU/max_uid < 40){ // 5% [1,13]
      value_size = value_size%13;
   }
   else if(uid*100LU/max_uid < 95){ //40+55, [14,300]
      value_size = value_size%(300-14) + 14;
   }
   else if(uid*100LU/max_uid <= 100){
      value_size = value_size%(4000-300) + 300;
   } else{
      //error 
      perror("incorrect parameters for value size generation");
      exit(-1);
   };
   //8-byte key + value size
   if(value_size==0){
         value_size = 1;
   }

   uint64_t item_size = value_size + 8;

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
_etc_get_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item = ctx;
   if(kverrno){
      printf("Get error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   _update_stat(ori_item);
   free(ori_item);
}

static void
_etc_put_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item = ctx;
   if(kverrno){
      printf("Put error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   _update_stat(ori_item);
   free(ori_item);
}

static void
_etc_scan_get_complete(void*ctx, struct kv_item* item, int kverrno){
   uint64_t sid = (uint64_t)ctx;
   if(kverrno){
      printf("Scan get error %d for sid:%lu\n",kverrno,sid);
   }
}

static void launch_etc_read_intensive(struct workload *w, bench_t b,uint64_t* processed, int id) {
   uint64_t nb_requests = w->nb_requests_per_thread;

   for(uint64_t i = 0; i < nb_requests; i++) {
      struct kv_item* item = create_unique_item_etc(etc_next(w), w->nb_items_in_db);

      // 5% write 95% read
      long random = uniform_next() % 100;
      if(random < 5) {
         kv_put_async(item,_etc_put_complete,item);
      } else{
         kv_get_async(item,_etc_get_complete,item);
      }
      *processed = i;
   }
}

static void launch_etc_write_intensive(struct workload *w, bench_t b,uint64_t* processed, int id) {
   uint64_t nb_requests = w->nb_requests_per_thread;

   for(uint64_t i = 0; i < nb_requests; i++) {
      struct kv_item* item = create_unique_item_etc(etc_next(w), w->nb_items_in_db);

      // 50% write 50% read
      long random = uniform_next() % 100;
      if(random < 50) {
         kv_put_async(item,_etc_put_complete,item);
      } else{
         kv_get_async(item,_etc_get_complete,item);
      }
      *processed = i;
   }
}

/* Pretty printing */
static const char *name_etc(bench_t w) {
   if(w==etc){
      return "ETC workload";
   }
   return "??";
}

static int handles_etc(bench_t w) {
   if(w==etc){
      return 1;
   }
   return 0;
}

static const char* api_name_etc(void) {
   return "ETC";
}

struct workload_api ETC = {
   .handles = handles_etc,
   .launch = launch_etc_read_intensive,
   .name = name_etc,
   .api_name = api_name_etc,
   .create_unique_item = create_unique_item_etc,
};
