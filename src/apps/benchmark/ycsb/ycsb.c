#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "common.h"
#include "item.h"
#include "kvutil.h"
#include "kvs.h"

static struct kv_item* _create_unique_item_ycsb(uint64_t uid) {
   size_t item_size = 1024;
   return create_unique_item(item_size, uid);
}

static struct kv_item* create_unique_item_ycsb(uint64_t uid, uint64_t max_uid) {
   return _create_unique_item_ycsb(uid);
}

/* Is the current request a get or a put? */
static int random_get_put(int test) {
   long random = uniform_next() % 100;
   switch(test) {
      case 0: // A
         return random >= 50;
      case 1: // B
         return random >= 95;
      case 2: // C
         return 0;
      case 3: // E
         return random >= 95;
   }
   printf("Invalid test type\n");
   exit(-1);
}

static void
_ycsb_put_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item = ctx;
   if(kverrno){
      printf("Put error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   free(ori_item);
}

static void
_ycsb_get_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item = ctx;
   if(kverrno){
      printf("Get error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   uint32_t ksize = ori_item->meta.ksize;
   uint32_t vsize = ori_item->meta.vsize;
   if(!memcpy(ori_item->data+ksize, item->data+ksize, vsize)){
      printf("Value mismatch, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   free(ori_item);
}

static void _launch_ycsb(int test, int nb_requests, int zipfian) {
   declare_periodic_count;
   for(int i = 0; i < nb_requests; i++) {
      struct kv_item *item;
      if(zipfian)
         item = _create_unique_item_ycsb(zipf_next());
      else
         item = _create_unique_item_ycsb(uniform_next());

      // In these tests we update with a given probability
      if(random_get_put(test)) {
         kv_put_async(item,_ycsb_put_complete,item);
      } else { // or we read
         kv_get_async(item,_ycsb_get_complete,item);
      }
      periodic_count(1000, "YCSB Load Injector (%lu%%)", i*100LU/nb_requests);
   }
}

static void
_ycsb_update_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item = ctx;
   if(kverrno){
      printf("Update error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   free(ori_item);
}

static void
_ycsb_scan_get_complete(void*ctx, struct kv_item* item, int kverrno){
   uint64_t cnt = (uint64_t)ctx;
   if(kverrno){
      printf("Scan read error, idx:%d, err:%d\n",cnt, kverrno);
   }
}

/* YCSB E */
static void _launch_ycsb_e(int test, int nb_requests, int zipfian) {
   declare_periodic_count;
   random_gen_t rand_next = zipfian?zipf_next:uniform_next;
   struct kv_iterator *it = kv_iterator_alloc();

   for(size_t i = 0; i < nb_requests; i++) {
      if(random_get_put(test)) { 
         // In this test we update with a given probability
         struct kv_item* item = _create_unique_item_ycsb(rand_next());
         kv_put_async(item,_ycsb_update_complete,item);
      } 
      else {  
         //scan
         struct kv_item* item = _create_unique_item_ycsb(rand_next());
         uint32_t scan_length = uniform_next()%99+1;

         if(!kv_iterator_seek(it,item)){
            printf("Error in seek item, key:%lu\n",*(uint64_t*)item->data);
            exit(-1);
         }

         for(uint64_t i = 0; i < scan_length; i++) {
            if(kv_iterator_next(it)){
               item = kv_iterator_item(it);
               kv_get_async(item, _ycsb_scan_get_complete, (void*)i);
            }
         }
      }
      periodic_count(1000, "YCSB Load Injector (scans) (%lu%%)", i*100LU/nb_requests);
   }
}

/* Generic interface */
static void launch_ycsb(struct workload *w, bench_t b) {
   switch(b) {
      case ycsb_a_uniform:
         return _launch_ycsb(0, w->nb_requests_per_thread, 0);
      case ycsb_b_uniform:
         return _launch_ycsb(1, w->nb_requests_per_thread, 0);
      case ycsb_c_uniform:
         return _launch_ycsb(2, w->nb_requests_per_thread, 0);
      case ycsb_e_uniform:
         return _launch_ycsb_e(3, w->nb_requests_per_thread, 0);
      case ycsb_a_zipfian:
         return _launch_ycsb(0, w->nb_requests_per_thread, 1);
      case ycsb_b_zipfian:
         return _launch_ycsb(1, w->nb_requests_per_thread, 1);
      case ycsb_c_zipfian:
         return _launch_ycsb(2, w->nb_requests_per_thread, 1);
      case ycsb_e_zipfian:
         return _launch_ycsb_e(3, w->nb_requests_per_thread, 1);
      default:
         printf("Unsupported workload\n");
         exit(-1);
   }
}

/* Pretty printing */
static const char *name_ycsb(bench_t w) {
   switch(w) {
      case ycsb_a_uniform:
         return "YCSB A - Uniform";
      case ycsb_b_uniform:
         return "YCSB B - Uniform";
      case ycsb_c_uniform:
         return "YCSB C - Uniform";
      case ycsb_e_uniform:
         return "YCSB E - Uniform";
      case ycsb_a_zipfian:
         return "YCSB A - Zipf";
      case ycsb_b_zipfian:
         return "YCSB B - Zipf";
      case ycsb_c_zipfian:
         return "YCSB C - Zipf";
      case ycsb_e_zipfian:
         return "YCSB E - Zipf";
      default:
         return "??";
   }
}

static int handles_ycsb(bench_t w) {
   switch(w) {
      case ycsb_a_uniform:
      case ycsb_b_uniform:
      case ycsb_c_uniform:
      case ycsb_e_uniform:
      case ycsb_a_zipfian:
      case ycsb_b_zipfian:
      case ycsb_c_zipfian:
      case ycsb_e_zipfian:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_ycsb(void) {
   return "YCSB";
}

struct workload_api YCSB = {
   .handles = handles_ycsb,
   .launch = launch_ycsb,
   .api_name = api_name_ycsb,
   .name = name_ycsb,
   .create_unique_item = create_unique_item_ycsb,
};
