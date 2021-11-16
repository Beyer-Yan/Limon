#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "common.h"
#include "item.h"
#include "kvutil.h"
#include "kvs.h"
#include "histogram.h"

static struct kv_item* _create_unique_item_ycsb(uint64_t uid) {
   size_t item_size = 1000;
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
      case 3: // D
         return random >= 95;
      case 4: // E
         return random >= 95;
      case 5: // F
         return random >= 50;
   }
   printf("Invalid test type\n");
   exit(-1);
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
_ycsb_put_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item = ctx;
   if(kverrno){
      printf("Put error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   _update_stat(ori_item);
   free(ori_item);
}

struct get_callback{
   struct kv_item* item;
   uint64_t* processed;
};

static void
_ycsb_get_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *ori_item =ctx;
   if(kverrno){
      printf("Get error, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   }
   //uint32_t ksize = ori_item->meta.ksize;
   //uint32_t vsize = ori_item->meta.vsize;
   //if(!memcpy(ori_item->data+ksize, item->data+ksize, vsize)){
   //   printf("Value mismatch, item key:%lu, err:%d\n",*(uint64_t*)ori_item->data, kverrno);
   //}
   _update_stat(ori_item);
   free(ori_item);
}

static void 
_launch_ycsb_common(int test, int nb_requests, int zipfian, uint64_t* processed, int id) {
   for(int i = 0; i < nb_requests; i++) {
      uint64_t next = zipfian ? zipf_next() : uniform_next();
      struct kv_item *item = _create_unique_item_ycsb(next);
      //struct get_callback* cb = malloc(sizeof(*cb));
      //cb->item = item;
      //cb->processed = processed;

      random_get_put(test) ? kv_put_async(item,_ycsb_put_complete,item) :
                             kv_get_async(item,_ycsb_get_complete,item);
      *processed = i;
   }
}

// YCSB D
static void
_launch_ycsb_d(int test, int nb_requests, int zipfian,uint64_t* processed, int id) {
   for(int i = 0; i < nb_requests; i++) {
      uint64_t next = random_get_put(test) ? latest_next(1) : latest_next(0);
      struct kv_item *item = _create_unique_item_ycsb(next);
      
      random_get_put(test) ? kv_put_async(item,_ycsb_put_complete,item) :
                             kv_get_async(item,_ycsb_get_complete,item);
      *processed = i;
   }
}

static void
_ycsb_scan_get_complete(void*ctx, struct kv_item* item, int kverrno){
   uint64_t sid = (uint64_t)ctx;
   if(kverrno){
      printf("Scan get error %d for sid:%lu\n",kverrno,sid);
   }
}

// YCSB E
static void 
_launch_ycsb_e(int test, int nb_requests, int zipfian,uint64_t* processed, int id) {
   random_gen_t rand_next = zipfian?zipf_next:uniform_next;

   for(int i = 0; i < nb_requests; i++) {
      if(random_get_put(test)) { 
         // In this test we update with a given probability
         struct kv_item* item = _create_unique_item_ycsb(rand_next());
         kv_put_async(item,_ycsb_put_complete,item);
      } 
      else {  
         //scan
         struct kv_item* item = _create_unique_item_ycsb(rand_next());
         uint32_t scan_length = uniform_next()%99+1;
         uint64_t sid_array[scan_length];
         uint64_t found = kv_scan(item,scan_length,sid_array);
         if(found){
            for(uint64_t i=0;i<found;i++){
               kv_get_with_sid_async(sid_array[i],_ycsb_scan_get_complete,sid_array[i]);
            }
         }
         free(item);

      }
      *processed = i;
   }
}

// YCSB F
static void
_launch_ycsb_f(int test, int nb_requests, int zipfian, uint64_t* processed,int id) {
   //the same as YCSB A
}

/* Generic interface */
static void launch_ycsb(struct workload *w, bench_t b,uint64_t* processed, int id) {
   switch(b) {
      case ycsb_a_uniform:
         _launch_ycsb_common(0, w->nb_requests_per_thread, 0, processed,id);
         break;
      case ycsb_b_uniform:
         _launch_ycsb_common(1, w->nb_requests_per_thread, 0,processed, id);
         break;
      case ycsb_c_uniform:
         _launch_ycsb_common(2, w->nb_requests_per_thread, 0, processed, id);
         break;
      case ycsb_d_uniform:
         _launch_ycsb_d(3, w->nb_requests_per_thread, 0,processed, id);
         break;
      case ycsb_e_uniform:
         _launch_ycsb_e(4, w->nb_requests_per_thread, 0,processed, id);
         break;
      case ycsb_f_uniform:
         _launch_ycsb_f(5, w->nb_requests_per_thread, 0,processed, id);
         break;
      case ycsb_a_zipfian:
         _launch_ycsb_common(0, w->nb_requests_per_thread, 1,processed, id);
         break;
      case ycsb_b_zipfian:
         _launch_ycsb_common(1, w->nb_requests_per_thread, 1,processed, id);
         break;
      case ycsb_c_zipfian:
         _launch_ycsb_common(2, w->nb_requests_per_thread, 1,processed, id);
         break;
      case ycsb_d_zipfian:
         _launch_ycsb_d(3, w->nb_requests_per_thread, 1,processed, id);
         break;
      case ycsb_e_zipfian:
         _launch_ycsb_e(4, w->nb_requests_per_thread, 1,processed, id);
         break;
      case ycsb_f_zipfian:
         _launch_ycsb_f(5, w->nb_requests_per_thread, 1,processed, id);
         break;
      default:
         printf("Unsupported workload\n");
         exit(-1);
   }
}

/* Pretty printing */
static const char *name_ycsb(bench_t w) {
   switch(w) {
      case ycsb_a_uniform:
         return "YCSB-A-Uniform";
      case ycsb_b_uniform:
         return "YCSB-B-Uniform";
      case ycsb_c_uniform:
         return "YCSB-C-Uniform";
      case ycsb_d_uniform:
         return "YCSB-D-Uniform";
      case ycsb_e_uniform:
         return "YCSB-E-Uniform";
      case ycsb_f_uniform:
         return "YCSB-F-Uniform";
      case ycsb_a_zipfian:
         return "YCSB-A-Zipf";
      case ycsb_b_zipfian:
         return "YCSB-B-Zipf";
      case ycsb_c_zipfian:
         return "YCSB-C-Zipf";
      case ycsb_d_zipfian:
         return "YCSB-D-Zipf";
      case ycsb_e_zipfian:
         return "YCSB-E-Zipf";
      case ycsb_f_zipfian:
         return "YCSB-F-Zipf";
      default:
         return "??";
   }
}

static int handles_ycsb(bench_t w) {
   switch(w) {
      case ycsb_a_uniform:
      case ycsb_b_uniform:
      case ycsb_c_uniform:
      case ycsb_d_uniform:
      case ycsb_e_uniform:
      case ycsb_f_uniform:
      case ycsb_a_zipfian:
      case ycsb_b_zipfian:
      case ycsb_c_zipfian:
      case ycsb_d_zipfian:
      case ycsb_e_zipfian:
      case ycsb_f_zipfian:
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
