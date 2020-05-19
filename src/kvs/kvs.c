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

// The key field of item  shall be filed
void 
kv_get_async(struct kv_item *item, kv_cb cb_fn, void* ctx){
    _assert_parameters(item,cb_fn);

    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = shard_id % g_kvs->nb_workers;
    worker_enqueue_get(g_kvs->workers[worker_id],shard_id,item,cb_fn,ctx);
}

// The whole item shall be filled
void 
kv_put_async(struct kv_item *item, kv_cb cb_fn, void* ctx){
    _assert_parameters(item,cb_fn);

    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = shard_id % g_kvs->nb_workers;
    worker_enqueue_put(g_kvs->workers[worker_id],shard_id,item,cb_fn,ctx);
}

// The key field of item shall be filed
void 
kv_delete_async(struct kv_item *item, kv_cb cb_fn, void* ctx){
    _assert_parameters(item,cb_fn);

    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = shard_id % g_kvs->nb_workers;
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
    volatile bool completed;
    uint32_t nb_workers;
    uint32_t item_idx;
    struct _scan_worker_ctx *ctx_array;
    struct kv_item *item_array;
};

struct kv_iterator* kv_iterator_alloc(void){
    assert(g_kvs!=NULL);

    uint32_t size = sizeof(struct kv_iterator) + 
                    g_kvs->nb_workers * sizeof(struct _scan_worker_ctx) +
                    g_kvs->nb_workers * MAX_ITEM_SIZE;

    struct kv_iterator *it = malloc(size);
    it->nb_workers = g_kvs->nb_workers;
    it->item_idx = UINT32_MAX;
    it->ctx_array = (struct _scan_worker_ctx *)(it+1);
    it->item_array = (struct kv_item*)(it->ctx_array + g_kvs->nb_workers);

    uint32_t i = 0;
    for(;i<g_kvs->nb_workers;i++){
        it->ctx_array[i].worker_id = i;
        it->ctx_array[i].it = it;
    }
    return it;
}

void kv_iterator_release(struct kv_iterator *it){
    assert(it!=NULL);
    free(it);
}

static void
_seek_cb_fn(void*ctx, struct kv_item* item, int kverrno){
    struct kv_iterator *it = ctx;
    if(kverrno!=-KV_ESUCCESS){
        it->item_idx = UINT32_MAX;
    }
    else{
        uint32_t ksize = item->meta.ksize;
        memcpy(it->item_array[0].data,item->data,ksize);
        it->item_array[0].meta.ksize = ksize;
        it->item_idx = 0;
    }
    it->completed = true;
}
bool kv_iterator_seek(struct kv_iterator *it, struct kv_item *item){
    assert(it!=NULL);
    _assert_parameters(item,_seek_cb_fn);
    
    it->completed = false;
    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = shard_id % g_kvs->nb_workers;
    worker_enqueue_seek(g_kvs->workers[worker_id],shard_id,item,_seek_cb_fn,it);
    while(!it->completed);
    return it->item_idx==UINT32_MAX ? false : true;
}

static void
_first_cb_fn(void*ctx, struct kv_item* item, int kverrno){
    struct _scan_worker_ctx *swctx = ctx;
    struct kv_iterator *it = swctx->it;
    swctx->kverrno = kverrno;
    swctx->completed = true;
    uint32_t worker_id = swctx->worker_id;

    if(kverrno==-KV_ESUCCESS){
        uint32_t ksize = item->meta.ksize;
        memcpy(it->item_array[worker_id].data,item->data,ksize);
        it->item_array[worker_id].meta.ksize = ksize;
    }
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

static void
_calc_least_item(struct kv_iterator *it){
    uint32_t i = 0;
    it->item_idx = UINT32_MAX;
    for(i=0;i>g_kvs->nb_workers;i++){
        if(it->ctx_array[i].kverrno==-KV_ESUCCESS){
            if(it->item_idx == -UINT32_MAX){
                it->item_idx = i;
            }
            else{
                uint8_t *key1 = it->item_array[it->item_idx].data;
                uint8_t *key2 = it->item_array[i].data;
                uint32_t len1 = it->item_array[it->item_idx].meta.ksize;
                uint32_t len2 = it->item_array[i].meta.ksize;
                if(_key_cmp(key1,len1,key2,len2)>0){
                    it->item_idx = i;
                }
            }
        }
    }
}
bool kv_iterator_first(struct kv_iterator *it){
    assert(it!=NULL);
    
    it->completed = false;
    uint32_t i = 0;
    for(;i<g_kvs->nb_workers;i++){
        struct _scan_worker_ctx *swctx  = &it->ctx_array[i];
        swctx->completed = false;
        worker_enqueue_first(g_kvs->workers[i],UINT32_MAX,NULL,_first_cb_fn,swctx);
    }
    while(!it->completed){
        bool res = true;
        for(i=0;i<g_kvs->nb_workers;i++){
            res &= it->ctx_array[i].completed;
        }
        if(res){
            it->completed = true;
        }
    }
    _calc_least_item(it);
    return it->item_idx==UINT32_MAX ? false : true;
}

static void
_next_cb_fn(void*ctx, struct kv_item* item, int kverrno){
    _first_cb_fn(ctx,item,kverrno);
}
bool kv_iterator_next(struct kv_iterator *it){
    assert(it!=NULL);
    assert(it->item_idx!=UINT32_MAX);

    it->completed = false;
    uint32_t i = 0;
    struct kv_item *item = &it->item_array[it->item_idx];
    for(;i<g_kvs->nb_workers;i++){
        struct _scan_worker_ctx *swctx  = &it->ctx_array[i];
        swctx->completed = false;
        worker_enqueue_next(g_kvs->workers[i],UINT32_MAX,item,_next_cb_fn,swctx);
    }
    while(!it->completed){
        bool res = true;
        for(i=0;i<g_kvs->nb_workers;i++){
            res &= it->ctx_array[i].completed;
        }
        if(res){
            it->completed = true;
        }
    } 
    _calc_least_item(it);
    return it->item_idx==UINT32_MAX ? false : true; 
}

//The function returns only the key field of a item. If you want to get the whole item data,
//you can issue a kv_get_async command.
struct kv_item* kv_iterator_item(struct kv_iterator *it){
    assert(it!=NULL);
    if(it->item_idx==UINT32_MAX){
        return NULL;
    }
    return &it->item_array[it->item_idx];
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
