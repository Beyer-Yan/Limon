#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <arpa/inet.h>
#include "common.h"
#include "item.h"
#include "kvutil.h"
#include "kvs.h"
#include "kverrno.h"
#include <arpa/inet.h>

static void 
pin_me_on(int core) {

   cpu_set_t cpuset;
   pthread_t thread = pthread_self();

   CPU_ZERO(&cpuset);
   CPU_SET(core, &cpuset);

   int s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
   if (s != 0){
       printf("core:%d,pin failed\n",core);
       exit(-1);
   }
}

static uint64_t htonll(uint64_t val){
   return (((uint64_t)htonl(val))<<32) + htonl(val>>32); 
}

/*
 * Create a workload item for the database
 */
struct kv_item *create_unique_item(uint64_t item_size, uint64_t uid) {
   struct kv_item *item = malloc(item_size + sizeof(struct item_meta));
   item->meta.ksize = 8;
   item->meta.vsize = item_size - 8;

   //make keys bytes sequence comparable.
   *(uint64_t*)(item->data) = htonll(uid);
   *(uint64_t*)(item->data + 8) = uid;
   
   return item;
}

struct kv_item* create_item_from_item(struct kv_item *item) {
   assert(item!=NULL);
   uint32_t size = sizeof(struct kv_item) + item->meta.ksize + item->meta.vsize;
   struct kv_item* new_item = malloc(size);
   memcpy(new_item,item,size);
   return new_item;
}

static struct kv_item *create_workload_item(struct workload *w) {
   const uint64_t key = (uint64_t)-10;
   const char *name = w->api->api_name();
   uint32_t key_size = 8;
   uint32_t value_size = strlen(name) + 1;

   struct kv_item *item = malloc(key_size + value_size + sizeof(struct item_meta));

   item->meta.ksize = key_size;
   item->meta.vsize = value_size;
   *(uint64_t*)(item->data) = key;
   strcpy(item->data+8,name);

   return item;
}

/************************************************/

struct rebuild_pdata {
   uint64_t id;
   uint64_t *pos;
   uint64_t start;
   uint64_t end;
   uint64_t nb_inserted;

   struct workload *w;
};

static void
_repopulate_item_cb(void*ctx, struct kv_item*item, int kverrno){
   assert(ctx);
   struct kv_item *x = (struct kv_item *)ctx;
   free(x);
}

static void *_repopulate_db_worker(void *pdata) {
   struct rebuild_pdata *data = pdata;

   pin_me_on(data->id);

   uint64_t *pos = data->pos;
   struct workload *w = data->w;
   struct workload_api *api = w->api;
   uint64_t start = data->start;
   uint64_t end = data->end;
   for(uint64_t i = start; i < end; i++) {
      
      struct kv_item *item = api->create_unique_item(pos[i], w->nb_items_in_db);
      kv_populate_async(item,_repopulate_item_cb,item);
      data->nb_inserted++;
   }
   return NULL;
}

static void
_add_db_flag_complete(void*ctx, struct kv_item* item, int kverrno){
   volatile int* finished = ctx;
   if(kverrno){
      printf("Error in add db flag, err:%d\n",kverrno);
      exit(-1);
   }
   *finished = 1;
}

static void
_repopulate_sync_data_cb(void*ctx, struct kv_item* item, int kverrno){
   volatile int* finished = ctx;
   if(kverrno){
      printf("Failed to sync population\n");
      exit(-1);    
   }
   *finished = 1;
}

static void* _compute_populate_stat(void* pdata){
   struct rebuild_pdata *pdata_arr = pdata;
   int nb_threads = pdata_arr[0].w->nb_load_injectors;
   uint64_t total = pdata_arr[0].w->nb_items_in_db;

   uint64_t last = 0;
   int time = 0;

   while(1){
      uint64_t requested = 0;
      for(int i = 0; i < nb_threads; i++) {
         requested += pdata_arr[i].nb_inserted;
      }
      uint64_t diff = requested - last;
      last = requested;
      time++;
      printf("Populating database %3ds %7lu ops/s (%lu%%)\n",time,diff,last*100lu/total);
      fflush(stdout);
      sleep(1);
   }

   return NULL;
}

void repopulate_db(struct workload *w) {
   uint64_t nb_inserts = w->nb_items_in_db;
   uint64_t *pos = NULL;
   volatile int finished = 0;

   printf("Adding identification key for the db\n");

   struct kv_item *workload_item = create_workload_item(w);
   kv_populate_async(workload_item,_add_db_flag_complete,&finished);
   while(!finished);
   free(workload_item);

   printf("Initializing big array to insert elements in random order to make the benchmark fair vs other systems)\n");
   pos = malloc(w->nb_items_in_db * sizeof(*pos));
   for(uint64_t i = 0; i < w->nb_items_in_db; i++)
      pos[i] = i;

   // To be fair to other systems, we shuffle items in the DB so that the DB is not fully sorted by luck
   printf("Start shuffling\n");
   kv_shuffle(pos, nb_inserts);
   printf("Shuffle  completes\n");

   pthread_t *threads = malloc(w->nb_load_injectors*sizeof(*threads));
   struct rebuild_pdata *pdata_arr = calloc(w->nb_load_injectors,sizeof(struct rebuild_pdata));

   for(int i = 0; i < w->nb_load_injectors; i++) {
      pdata_arr[i].id = w->start_core + i;
      pdata_arr[i].start = (w->nb_items_in_db / w->nb_load_injectors)*i;
      pdata_arr[i].end = (w->nb_items_in_db / w->nb_load_injectors)*(i+1);
      if(i == w->nb_load_injectors - 1)
         pdata_arr[i].end = w->nb_items_in_db;
      pdata_arr[i].w = w;
      pdata_arr[i].pos = pos;
      pdata_arr[i].nb_inserted = 0;
      pthread_create(&threads[i], NULL, _repopulate_db_worker, &pdata_arr[i]);
   }

   pthread_t stats_thread;
   pthread_create(&stats_thread, NULL, _compute_populate_stat, pdata_arr);

   //wait the finishing of db repopulating.
   for(int i = 0; i < w->nb_load_injectors; i++){
      pthread_join(threads[i], NULL);
   }

   pthread_cancel(stats_thread);
   pthread_join(stats_thread, NULL);

   finished = 0;

   kv_populate_async(NULL,_repopulate_sync_data_cb,(void*)&finished);
   while(!finished);
   printf("Populating database completes\n");

   free(threads);
   free(pos);
   free(pdata_arr);
}

/**********************************************************************/

static void
_check_db_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *workload_item = ctx;
   if(kverrno==-KV_EITEM_NOT_EXIST){
      printf("Couldn't determine if workload corresponds to the benchmark --- please wipe DB before benching!\n");
      exit(-1);
   }
   if(!kverrno){
      if(memcmp(workload_item->data + 8, item->data + 8, workload_item->meta.vsize)){
         printf("Wrong workload name, name in db:%s, yours:%s! Please format or repopulate the kvs\n",item->data + 8,workload_item->data);
         exit(-1);
      }
   }
   workload_item->meta.ksize = 0;
}

static int _prepare_run_workload(struct workload *w){
   uint64_t nb_items = kvs_get_nb_items();
   uint64_t nb_inserts = ( nb_items > w->nb_items_in_db)?0:(w->nb_items_in_db - nb_items);
   volatile int finished = 0;

   if(nb_inserts != w->nb_items_in_db) { 
      // Database at least partially populated
      // Check that the items correspond to the workload
      struct kv_item *workload_item = create_workload_item(w);
      kv_get_async(workload_item,_check_db_complete,workload_item);
      while(!workload_item->meta.ksize);

      free(workload_item);
   }

   //Tell the worker the db has been populated
   finished = 0;
   kv_populate_async(NULL,_repopulate_sync_data_cb,(void*)&finished);
   while(!finished);

   if(nb_inserts == 0) {
      return 0;
   }

   if(nb_items == 0 || nb_items != w->nb_items_in_db+1) {
      //need reshuffling
      printf("The database contains %lu elements but the benchmark is configured to use %lu. Please delete and repopulate the DB first.\n", nb_items, w->nb_items_in_db);
      exit(-1);
   }

   return 0;
}

/*
 * Generic worklad API.
 */
struct thread_data {
   uint64_t id;
   struct workload *workload;
   bench_t benchmark;

   uint64_t nb_requests;
   uint64_t nb_injected;
};

struct workload_api *get_api(bench_t b) {
   if(YCSB.handles(b))
      return &YCSB;
   if(UDB.handles(b))
      return &UDB;
   if(ETC.handles(b))
      return &ETC;

   printf("Unknown workload for benchmark!\n");
   exit(-1);
}

static pthread_barrier_t barrier;
static void* _do_workload_thread(void *pdata) {
   struct thread_data *data = pdata;

   init_seed();
   pin_me_on(data->id);
   pthread_barrier_wait(&barrier);

   data->workload->api->launch(data->workload,data->benchmark, &data->nb_injected, data->id);

   return NULL;
}

static void* _compute_stat(void* pdata){
   struct thread_data *thread_data = pdata;
   const char* w_name = thread_data[0].workload->api->name(thread_data[0].benchmark);
   int nb_threads = thread_data[0].workload->nb_load_injectors;
   uint64_t total = thread_data[0].workload->nb_requests;

   uint64_t last = 0;
   int time = 0;

   while(1){
      uint64_t requested = 0;
      for(int i = 0; i < nb_threads; i++) {
         requested += thread_data[i].nb_injected;
      }
      uint64_t diff = requested - last;
      last = requested;
      time++;
      printf("Run workload %s %3ds %7lu ops/s (%lu%%)\n",w_name,time,diff,last*100lu/total);
      fflush(stdout);
      sleep(1);
   }

   return NULL;
}

void run_workload(struct workload *w, bench_t b) {
   _prepare_run_workload(w);

   struct thread_data *pdata = malloc(w->nb_load_injectors*sizeof(*pdata));

   w->nb_requests_per_thread = w->nb_requests / w->nb_load_injectors;
   pthread_barrier_init(&barrier, NULL, w->nb_load_injectors);

   if(!w->api->handles(b)){
      printf("The database has not been configured to run this benchmark!"); 
      exit(-1);
      return;
   }

   pthread_t *threads = malloc(w->nb_load_injectors*sizeof(*threads));
   for(int i = 0; i < w->nb_load_injectors; i++) {
      pdata[i].id = w->start_core + i;
      pdata[i].workload = w;
      pdata[i].benchmark = b;
      pdata[i].nb_requests = w->nb_requests_per_thread;
      pdata[i].nb_injected = 0;

      pthread_create(&threads[i], NULL, _do_workload_thread, &pdata[i]);
   }

   pthread_t stats_thread;
   pthread_create(&stats_thread, NULL, _compute_stat, pdata);
   
   for(int i = 0; i < w->nb_load_injectors; i++){
      pthread_join(threads[i], NULL);
   }

   pthread_cancel(stats_thread);
   pthread_join(stats_thread, NULL);
      
   free(threads);
   free(pdata);
}

