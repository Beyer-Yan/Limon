#include "kvs_internal.h"

static inline uint32_t
_hash_item_to_shard(struct kv_item *item){
    return kv_hash(item->data, item->meta.ksize,g_kvs->nb_shards);
}

static inline void
_assert_parameters(struct kv_item *item, kv_cb cb_fn){
    assert(g_kvs!=NULL);
    assert(item!=NULL);
    assert(cb_fn!=NULL);
    assert(item->meta.ksize>0);
    assert(item->meta.ksize<=g_kvs->max_key_length);
}

static uint32_t
_hash_shard_to_worker(uint32_t shard_id){
    uint32_t nb_shards_per_worker = g_kvs->nb_shards/g_kvs->nb_workers;
    return shard_id/nb_shards_per_worker;
}

// The key field of item  shall be filed
void 
kv_get_async(struct kv_item *item, kv_cb cb_fn, void* ctx){
    _assert_parameters(item,cb_fn);

    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = _hash_shard_to_worker(shard_id);
    worker_enqueue_get(g_kvs->workers[worker_id],shard_id,item,cb_fn,ctx);
}

// The whole item shall be filled
void 
kv_put_async(struct kv_item *item, kv_cb cb_fn, void* ctx){
    _assert_parameters(item,cb_fn);

    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = _hash_shard_to_worker(shard_id);
    worker_enqueue_put(g_kvs->workers[worker_id],shard_id,item,cb_fn,ctx);
}

// The key field of item shall be filed
void 
kv_delete_async(struct kv_item *item, kv_cb cb_fn, void* ctx){
    _assert_parameters(item,cb_fn);

    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = _hash_shard_to_worker(shard_id);
    worker_enqueue_delete(g_kvs->workers[worker_id],shard_id,item,cb_fn,ctx);
}

struct _scan_worker_ctx{
    uint32_t worker_id;
    volatile bool completed;
    int kverrno;
    struct kv_iterator *it;
};

//@todo A big-top heap may be the best choice
struct kv_iterator{
    uint32_t nb_workers;
    uint32_t item_cursor;
    struct _scan_worker_ctx **sorted_ctx;
    struct _scan_worker_ctx *ctx_array;
    struct kv_item *item_array;
};

struct kv_iterator* kv_iterator_alloc(void){
    assert(g_kvs!=NULL);

    uint32_t size = sizeof(struct kv_iterator) + 
                    g_kvs->nb_workers *sizeof(struct _scan_worker_ctx *) +
                    g_kvs->nb_workers * sizeof(struct _scan_worker_ctx) +
                    g_kvs->nb_workers * MAX_ITEM_SIZE;

    struct kv_iterator *it = malloc(size);
    it->nb_workers = g_kvs->nb_workers;
    it->item_cursor = UINT32_MAX;
    it->sorted_ctx = (struct _scan_worker_ctx **)(it+1);
    it->ctx_array = (struct _scan_worker_ctx *)(it->sorted_ctx + g_kvs->nb_workers);
    it->item_array = (struct kv_item*)(it->ctx_array + g_kvs->nb_workers);

    uint32_t i = 0;
    for(;i<g_kvs->nb_workers;i++){
        it->ctx_array[i].worker_id = i;
        it->ctx_array[i].it = it;
        it->sorted_ctx[i] = &it->ctx_array[i];
    }
    return it;
}

void kv_iterator_release(struct kv_iterator *it){
    assert(it!=NULL);
    free(it);
}

static inline int
_key_cmp(const uint8_t *key1,uint32_t len1,const uint8_t *key2, uint32_t len2){
    uint32_t n = len1<len2 ? len1 : len2;
    int res = strncmp(key1,key2,n);
    if(!res){
        res = len1==len2 ? 0 : (len1<len2 ? -1 : 1 );
    }
    return res;
}

static int
_sort_compare(const void*a, const void*b){
    struct _scan_worker_ctx * swctx1 = a;
    struct _scan_worker_ctx * swctx2 = b;
    struct kv_iterator* it = swctx1->it;

    const uint8_t* key1 = it->item_array[swctx1->worker_id].data;
    uint32_t len1 = it->item_array[swctx1->worker_id].meta.ksize;

    const uint8_t* key2 = it->item_array[swctx2->worker_id].data;
    uint32_t len2 = it->item_array[swctx2->worker_id].meta.ksize;

    return _key_cmp(key1,len1,key2,len2);
}

static void
_iter_cb_fn(void*ctx, struct kv_item* item, int kverrno){
    struct _scan_worker_ctx * swctx = ctx;
    if(!kverrno){
        //Seek successes;
        uint32_t item_id = swctx->worker_id;
        struct kv_iterator* it = swctx->it;
        uint32_t ksize = item->meta.ksize;

        memcpy(it->item_array[item_id].data,item->data,ksize);
        it->item_array[item_id].meta.ksize = ksize;
    }
    swctx->kverrno = kverrno;
    swctx->completed = true;
}

bool kv_iterator_seek(struct kv_iterator *it, struct kv_item *item){
    assert(it!=NULL);
    _assert_parameters(item,_iter_cb_fn);
    
    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = _hash_shard_to_worker(shard_id);
    worker_enqueue_seek(g_kvs->workers[worker_id],shard_id,item,_iter_cb_fn,&it->ctx_array[worker_id]);
    while(!it->ctx_array[worker_id].completed);
    
    if(it->ctx_array[worker_id].kverrno){
        //seek fails;
        return false;
    }

    //Now set the cursor for other workers.
    uint32_t i=0;
    for(;i<it->nb_workers;i++){
        if(i!=worker_id){
            struct _scan_worker_ctx *swctx  = &it->ctx_array[i];
            swctx->completed = false;
            swctx->kverrno = 0;
            worker_enqueue_next(g_kvs->workers[i],UINT32_MAX, &it->item_array[worker_id],_iter_cb_fn,swctx);
        }
    }

    bool res = true;
    while(res){
        for(i=0;i<g_kvs->nb_workers;i++){
            res &= it->ctx_array[i].completed;
        }
    }

    //check the valid items.
    it->item_cursor = 0;;
    for(i=0;i<it->nb_workers;i++){
        struct _scan_worker_ctx *swctx  = &it->ctx_array[i];
        if(!swctx->kverrno){
            it->sorted_ctx[it->item_cursor] = swctx;
            it->item_cursor++;
        }
    }

    //Do pre-sorting, 
    qsort(it->sorted_ctx,it->item_cursor+1,sizeof(struct _scan_worker_ctx*),_sort_compare);
    
    //All cursors have been set.
    return true;
}

bool kv_iterator_first(struct kv_iterator *it){
    assert(it!=NULL);

    //set the cursor for all workers.
    uint32_t i=0;
    for(;i<it->nb_workers;i++){
        struct _scan_worker_ctx *swctx  = &it->ctx_array[i];
        swctx->completed = false;
        swctx->kverrno = 0;
        worker_enqueue_first(g_kvs->workers[i],UINT32_MAX, NULL,_iter_cb_fn,swctx);
    }

    bool res = true;
    while(res){
        for(i=0;i<g_kvs->nb_workers;i++){
            res &= it->ctx_array[i].completed;
        }
    }

    //check the valid items.
    it->item_cursor = 0;;
    for(i=0;i<it->nb_workers;i++){
        struct _scan_worker_ctx *swctx  = &it->ctx_array[i];
        if(!swctx->kverrno){
            it->sorted_ctx[it->item_cursor] = swctx;
            it->item_cursor++;
        }
    }

    //Do pre-sorting, 
    qsort(it->sorted_ctx,it->item_cursor+1,sizeof(struct _scan_worker_ctx*),_sort_compare);
    
    //All cursors have been set.
    return true;
}

static void
_next_cb_fn(void*ctx, struct kv_item* item, int kverrno){
   struct _scan_worker_ctx *swctx = ctx;
   struct kv_iterator *it = swctx->it;
   if(!kverrno){
        uint32_t item_id = swctx->worker_id;
        struct kv_iterator* it = swctx->it;
        uint32_t ksize = item->meta.ksize;

        memcpy(it->item_array[item_id].data,item->data,ksize);
        it->item_array[item_id].meta.ksize = ksize;

        //sort the item. I need not fully sort them, just check the current.
        qsort(it->sorted_ctx,it->item_cursor+1,sizeof(struct _scan_worker_ctx*),_sort_compare);
   }
   else{
       //Exhausted the current worker; 
       it->item_cursor--;
   }
   swctx->kverrno = kverrno;
   swctx->completed = true;
}
bool kv_iterator_next(struct kv_iterator *it){
    assert(it!=NULL);

    //Get the maximum item.
    uint32_t worker_id = it->sorted_ctx[it->item_cursor]->worker_id;
    uint32_t item_id = worker_id;

    struct _scan_worker_ctx *swctx  = &it->ctx_array[worker_id];
    struct kv_item* item = &it->item_array[item_id];

    //I need not send request for all workers.
    swctx->completed = false;
    swctx->kverrno = 0;
    worker_enqueue_next(g_kvs->workers[worker_id],UINT32_MAX,item,_next_cb_fn,swctx);

    while(!swctx->completed);

    return (swctx->kverrno) ? false : true;
}

//get the whole item data, just issue a kv_get_async command.
struct kv_item* kv_iterator_item(struct kv_iterator *it){
    assert(it!=NULL);
    if(it->item_cursor==UINT32_MAX){
        return NULL;
    }

    uint32_t worker_id = it->sorted_ctx[it->item_cursor]->worker_id;
    uint32_t item_id = worker_id;
    struct kv_item* item = &it->item_array[item_id];

    return item;
}

struct slab_statistics* kvs_get_slab_statistcs(void){
    struct slab_statistics* res;
    uint32_t nb_total_slabs = g_kvs->nb_shards*g_kvs->shards[0].nb_slabs;
    uint64_t size  = sizeof(*res) + sizeof(*res->slabs)*nb_total_slabs;

    if(!g_kvs){
        return NULL;
    }

    res = malloc(size);
    if(!res){
        return NULL;
    }
    res->nb_shards = g_kvs->nb_shards;
    res->nb_slabs_per_shard = g_kvs->shards[0].nb_slabs;
    uint32_t i = 0;
    for(;i<nb_total_slabs;i++){
        uint32_t shard_idx = i/res->nb_slabs_per_shard;
        uint32_t slab_idx  = i%res->nb_slabs_per_shard;
        struct slab *slab = &g_kvs->shards[shard_idx].slab_set[slab_idx];
        
        res->slabs[i].slab_size = slab->slab_size;
        res->slabs[i].nb_slots = slab->reclaim.nb_total_slots;
        res->slabs[i].nb_free_slots = slab->reclaim.nb_free_slots;
    }
    return res;
}

struct kvs_runtime_statistics* kvs_get_runtime_statistics(void){ 
    struct kvs_runtime_statistics* res;
    uint64_t size = sizeof(*res) + sizeof(*res->ws)*g_kvs->nb_workers;

    if(!g_kvs){
        return NULL;
    }

    res = malloc(size);
    if(!res){
        return NULL;
    }

    res->nb_worker = g_kvs->nb_workers;
    uint32_t i = 0;
    for(;i<g_kvs->nb_workers;i++){
        worker_get_statistics(g_kvs->workers[i],&res->ws[i]);
    }

    return res;
}

uint64_t kvs_get_nb_items(void){
    if(!g_kvs){
        return 0;
    }

    struct slab_statistics* ss = kvs_get_slab_statistcs();
    if(!ss){
        return 0;
    }

    uint64_t nb_items = 0;
    uint64_t i=0;
    for(;i<ss->nb_shards*ss->nb_slabs_per_shard;i++){
        nb_items += ss->slabs[i].nb_slots - ss->slabs[i].nb_free_slots;
    }
    free(ss);
    return nb_items;
}
