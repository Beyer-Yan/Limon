#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <arpa/inet.h>
#include "common.h"
#include "item.h"
#include "kvutil.h"
#include "kvs.h"
#include "kverrno.h"

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

/*
 * Create a workload item for the database
 */
struct kv_item *create_unique_item(uint64_t item_size, uint64_t uid) {
   struct kv_item *item = malloc(item_size + sizeof(struct item_meta));
   item->meta.ksize = 8;
   item->meta.vsize = item_size - sizeof(struct item_meta) - 8;

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

/* We also store an item in the database that says if the database has been populated for YCSB, PRODUCTION, or another workload. */
struct kv_item *create_workload_item(struct workload *w) {
   const uint64_t key = (uint64_t)-10;
   const char *name = w->api->api_name(); // YCSB or PRODUCTION?
   uint32_t key_size = 8;
   uint32_t value_size = strlen(name) + 1;

   struct kv_item *item = malloc(key_size + value_size + sizeof(struct item_meta));

   item->meta.ksize = key_size;
   item->meta.vsize = value_size;
   *(uint64_t*)(item->data) = key;
   strcpy(item->data+8,name);

   return item;
}


struct rebuild_pdata {
   uint64_t id;
   uint64_t *pos;
   uint64_t start;
   uint64_t end;
   struct workload *w;
};

static void
_repopulate_item_add_cb(void*ctx, struct kv_item*item, int kverrno){

}

void *repopulate_db_worker(void *pdata) {
   declare_periodic_count;
   struct rebuild_pdata *data = pdata;

   pin_me_on(data->id);

   uint64_t *pos = data->pos;
   struct workload *w = data->w;
   struct workload_api *api = w->api;
   uint64_t start = data->start;
   uint64_t end = data->end;
   for(uint64_t i = start; i < end; i++) {
      
      struct kv_item *item = api->create_unique_item(pos[i], w->nb_items_in_db);
      kv_put_async(item,_repopulate_item_add_cb,item);
      periodic_count(1000, "Repopulating database, w:%lu, (%lu%%)", data->id ,100LU-(end-i)*100LU/(end - start));
   }
   free(data);
   return NULL;
}

static volatile int finished = 0;
static volatile int ok = 0;

static void
_check_db_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *workload_item = ctx;
   if(kverrno==-KV_EITEM_NOT_EXIST){
      printf("Running a benchmark on a pre-populated DB, but couldn't determine if items in the DB correspond to the benchmark --- please wipe DB before benching!\n");
   }
   if(!kverrno){
      if(!memcmp(workload_item->data + 8, item->data + 8, workload_item->meta.vsize)){
         ok = 1;
      }
      else{
         printf("Wrong workload name, name in db:%s, yours:%s!\n",item->data + 8,workload_item->data);
      }
   }
   free(workload_item);
   finished = 1;
}

static void
_check_db(struct workload *w){
   struct kv_item *workload_item = create_workload_item(w);
   finished = 0;
   ok = 0;
   kv_get_async(workload_item,_check_db_complete,workload_item);
   while(!finished);
   if(!ok){
      printf("Can not resolve the workload type, please format the kvs\n");
      exit(-1);
   }
}

static void
_add_db_flag_complete(void*ctx, struct kv_item* item, int kverrno){
   struct kv_item *workload_item = ctx;
   if(kverrno){
      printf("Error in put item:%lu, err:%d\n", *(uint64_t*)workload_item->data,kverrno);
   }
   else{
      ok = 1;
   }
   free(workload_item);
   finished = 1;
}

static void
_add_db_flag(struct workload *w){
   struct kv_item *workload_item = create_workload_item(w);
   finished = 0;
   ok = 0;
   kv_put_async(workload_item,_add_db_flag_complete,workload_item);
   while(!finished);
   if(!ok){
      printf("Failed to add workload flag\n");
      exit(-1);
   }
}

void repopulate_db(struct workload *w) {
   uint64_t nb_items = kvs_get_nb_items();
   uint64_t nb_inserts = ( nb_items > w->nb_items_in_db)?0:(w->nb_items_in_db - nb_items);

   if(nb_inserts != w->nb_items_in_db) { 
      // Database at least partially populated
      // Check that the items correspond to the workload
      _check_db(w);
   }

   if(nb_inserts == 0) {
      return;
   }

   // Say that this database is for that workload.
   if(nb_items == 0) {
      _add_db_flag(w);
   } else {
      nb_items--; // do not count the workload_item
   }

   if(nb_items != 0 && nb_items != w->nb_items_in_db) {
      /*
       * Because we shuffle elements, we don't really want to start with a small database and have all the higher order elements at the end, that would be cheating.
       * Plus, we insert database items at random positions (see shuffle below) and I am too lazy to implement the logic of doing the shuffle minus existing elements.
       */
      printf("The database contains %lu elements but the benchmark is configured to use %lu. Please delete the DB first.\n", nb_items, w->nb_items_in_db);
      exit(-1);
   }

   uint64_t *pos = NULL;

   printf("Initializing big array to insert elements in random order... This might take a while. (Feel free to comment but then the database will be sorted and scans much faster -- unfair vs other systems)\n");
   pos = malloc(w->nb_items_in_db * sizeof(*pos));
   for(uint64_t i = 0; i < w->nb_items_in_db; i++)
      pos[i] = i;

   // To be fair to other systems, we shuffle items in the DB so that the DB is not fully sorted by luck
   printf("Start shuffling\n");
   kv_shuffle(pos, nb_inserts);
   printf("Shuffle  completes\n");

   pthread_t *threads = malloc(w->nb_load_injectors*sizeof(*threads));

   for(int i = 0; i < w->nb_load_injectors; i++) {
      struct rebuild_pdata *pdata = calloc(1,sizeof(*pdata));
      pdata->id = w->start_core + i;
      pdata->start = (w->nb_items_in_db / w->nb_load_injectors)*i;
      pdata->end = (w->nb_items_in_db / w->nb_load_injectors)*(i+1);
      if(i == w->nb_load_injectors - 1)
         pdata->end = w->nb_items_in_db;
      pdata->w = w;
      pdata->pos = pos;
      pthread_create(&threads[i], NULL, repopulate_db_worker, pdata);
   }

   //wait the finishing of db repopulating.
   for(int i = 0; i < w->nb_load_injectors; i++){
      pthread_join(threads[i], NULL);
   }
   free(threads);
   free(pos);
}

/*
 * Generic worklad API.
 */
struct thread_data {
   uint64_t id;
   struct workload *workload;
   bench_t benchmark;
};

struct workload_api *get_api(bench_t b) {
   if(YCSB.handles(b))
      return &YCSB;
   if(PRODUCTION.handles(b))
      return &PRODUCTION;
   printf("Unknown workload for benchmark!\n");
   exit(-1);
}

static pthread_barrier_t barrier;
void* do_workload_thread(void *pdata) {
   struct thread_data *data = pdata;

   init_seed();
   pin_me_on(data->id);
   pthread_barrier_wait(&barrier);

   data->workload->api->launch(data->workload, data->benchmark, data->id);

   return NULL;
}

void run_workload(struct workload *w, bench_t b) {
   struct thread_data *pdata = malloc(w->nb_load_injectors*sizeof(*pdata));

   w->nb_requests_per_thread = w->nb_requests / w->nb_load_injectors;
   pthread_barrier_init(&barrier, NULL, w->nb_load_injectors);

   if(!w->api->handles(b)){
      printf("The database has not been configured to run this benchmark! (Are you trying to run a production benchmark on a database configured for YCSB?)"); 
      exit(-1);
      return;
   }

   pthread_t *threads = malloc(w->nb_load_injectors*sizeof(*threads));
   for(int i = 0; i < w->nb_load_injectors; i++) {
      pdata[i].id = w->start_core + i;
      pdata[i].workload = w;
      pdata[i].benchmark = b;
      pthread_create(&threads[i], NULL, do_workload_thread, &pdata[i]);
   }
   
   for(int i = 0; i < w->nb_load_injectors; i++){
      pthread_join(threads[i], NULL);
   }
      
   free(threads);
   free(pdata);
}

