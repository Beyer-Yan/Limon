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
    struct worker_scan_result* scan_res;
    struct kv_iterator *it;
};

//@todo A big-top heap may be the best choice
struct kv_iterator{
    uint32_t nb_workers;
    uint32_t batch_size;
    uint32_t nb_items;
    uint32_t cursor;

    //when issuing a seek command, no item will returned.
    //I have to store it into other place in case that the
    //user releases the item.
    struct kv_item* seek_item;

    struct _scan_worker_ctx *ctx_array;
    struct kv_item **sorted_item;
};

struct kv_iterator* kv_iterator_alloc(int batch_size){
    assert(g_kvs!=NULL);

    if(batch_size&(batch_size-1)){
        SPDK_ERRLOG("batch size must be the value of power-of-two\n");
        assert(0);
    }

    uint32_t size = sizeof(struct kv_iterator) + 
                    g_kvs->nb_workers * sizeof(struct _scan_worker_ctx) +
                    sizeof(struct kv_item*)*(batch_size + 1) +
                    MAX_SLAB_SIZE;

    struct kv_iterator *it = malloc(size);
    assert(it);

    it->nb_workers = g_kvs->nb_workers;
    it->batch_size = batch_size;
    it->nb_items   = 0;
    it->cursor     = 0;
    it->seek_item  = NULL;
    it->ctx_array  = (struct _scan_worker_ctx *)(it+1);
    it->sorted_item = (struct kv_item **)(it->ctx_array + g_kvs->nb_workers);
    it->seek_item = (struct kv_item*)(it->sorted_item + batch_size+1 );

    uint32_t i = 0;
    for(;i<g_kvs->nb_workers;i++){
        it->ctx_array[i].worker_id = i;
        it->ctx_array[i].it = it;
        it->ctx_array[i].scan_res = NULL;
    }
    return it;
}

static void
_reset_scan_res(struct kv_iterator *it){
    uint32_t i=0,j=0;
    for(i=0;i<g_kvs->nb_workers;i++){
        struct _scan_worker_ctx *swctx = &it->ctx_array[i];
        if(swctx->scan_res){
            for(j=0;j<swctx->scan_res->nb_items;j++){
                free(swctx->scan_res->items[j]);
            }
            free(swctx->scan_res);
            swctx->scan_res = NULL;
            swctx->kverrno = 0;
            swctx->completed = false;
        }
    }
    it->cursor    = 0;
    it->nb_items  = 0;   
}

void kv_iterator_release(struct kv_iterator *it){
    assert(it!=NULL);
    _reset_scan_res(it);
    free(it);
}

static inline int
_item_cmp(const struct kv_item* item0,const struct kv_item* item1 ){
    
    //A null item is less than any item.
    if(!item1){
        return 1;
    }
    if(!item0){
        return -1;
    }

    const uint8_t *key0 = item0->data;
    const uint8_t *key1 = item1->data;
    uint32_t len0   = item0->meta.ksize;
    uint32_t len1   = item1->meta.ksize;

    uint32_t n = len0<len1 ? len0 : len1;
    int res = memcmp(key0,key1,n);
    if(!res){
        res = len0==len1 ? 0 : (len0<len1 ? -1 : 1 );
    }
    return res;
}

static void
_seek_cb_fn(void*ctx, struct kv_item* item, int kverrno){
    struct _scan_worker_ctx * swctx = ctx;
    swctx->kverrno = kverrno;
    swctx->completed = true;
}

bool kv_iterator_seek(struct kv_iterator *it, struct kv_item *item){
    assert(it!=NULL);
    _assert_parameters(item,_seek_cb_fn);

    _reset_scan_res(it);
    
    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = _hash_shard_to_worker(shard_id);
    worker_enqueue_seek(g_kvs->workers[worker_id],item,_seek_cb_fn,&it->ctx_array[worker_id]);

    while(!it->ctx_array[worker_id].completed);
    
    if(it->ctx_array[worker_id].kverrno){
        //seek fails;
        return false;
    }

    //copy the item into seek_item.
    //I will use it in the "next" operation. 
    memcpy(it->seek_item->data,item->data,item->meta.ksize);
    it->seek_item->meta.ksize = item->meta.ksize;

    it->nb_items = 1;
    it->sorted_item[0] = it->seek_item;
    it->cursor = 0;

    //All cursors have been set.
    return true;
}

static void
_scan_merge(struct kv_iterator *it){
    uint32_t i=0, j=0;

    for(;i<it->nb_workers;i++){
        //get the max item from all scan contexts
        struct _scan_worker_ctx * swctx = &it->ctx_array[i];
        if(swctx->scan_res){
            for(j=0;j<swctx->scan_res->nb_items;j++){
                it->sorted_item[it->nb_items] = swctx->scan_res->items[j];
                it->nb_items++;
            }
        }
    }
    
    //A merge sort may be the best choice, which will be implemented in the next
    //version.
    qsort(it->sorted_item,it->nb_items,sizeof(struct kv_item*),_item_cmp);
    
    /*
    //record the scaning index for each scan context
    //sort the items. 
    uint32_t item_idx[it->nb_workers];
    memset(item_idx,0,it->nb_workers);

    struct kv_item *max_item = NULL;
    uint32_t max_worker_idx = 0;
    uint32_t max_scan_idx  = 0;

    for(i=0;i<max_nb_items;i++){
        //find the max item in the column.
        for(j=0;j<it->nb_workers;j++){
            struct _scan_worker_ctx * swctx = &it->ctx_array[i];
            if(item_idx[j]<swctx->scan_res->nb_items){
                struct kv_item* cur_item = swctx->scan_res->items[item_idx[j]];
                if(_item_cmp(cur_item,max_item)){
                    max_item = cur_item;
                    max_worker_idx = j;
                    max_scan_idx = item_idx[j];
                }
            }
        }
        assert(max_item);
        item_idx[max_worker_idx]++;
        it->sorted_item[it->nb_items] = max_item;
        it->nb_items++;
    }
    */
}

static void
_iter_cb_fn(void*ctx, struct worker_scan_result* scan_res, int kverrno){
    struct _scan_worker_ctx * swctx = ctx;

    if(!kverrno){
        swctx->scan_res = scan_res;
    }

    swctx->kverrno = kverrno;
    swctx->completed = true;
}

bool kv_iterator_first(struct kv_iterator *it){
    assert(it!=NULL);
    
    _reset_scan_res(it);
    //set the cursor for all workers.
    uint32_t i=0;
    for(;i<it->nb_workers;i++){
        struct _scan_worker_ctx *swctx  = &it->ctx_array[i];
        uint32_t per_worker_batch = it->batch_size/it->nb_workers;
        swctx->completed = false;
        swctx->kverrno = 0;
        worker_enqueue_first(g_kvs->workers[i],_iter_cb_fn,per_worker_batch,swctx);
    }

    bool res = true;
    while(res){
        for(i=0;i<g_kvs->nb_workers;i++){
            res &= it->ctx_array[i].completed;
        }
    }

    _scan_merge(it);
    //All cursors have been set.
    return true;
}

bool kv_iterator_next(struct kv_iterator *it){
    assert(it!=NULL);
    assert(it->nb_items>0);

    if(it->cursor<it->nb_items-1){
        it->cursor++;
        return true;
    }

    //Get the last item.
    struct kv_item* cursor_item = it->sorted_item[it->cursor];
    memcpy(it->seek_item,cursor_item,cursor_item->meta.ksize);

    //Get the next batch of items
    _reset_scan_res(it);

    uint32_t i=0;
    for(;i<it->nb_workers;i++){
        struct _scan_worker_ctx *swctx  = &it->ctx_array[i];
        uint32_t per_worker_batch = it->batch_size/it->nb_workers;
        swctx->completed = false;
        swctx->kverrno = 0;
        worker_enqueue_next(g_kvs->workers[i],cursor_item,_iter_cb_fn,per_worker_batch,swctx);
    }

    //I need not send request for all workers.
    bool res = true;
    while(res){
        for(i=0;i<g_kvs->nb_workers;i++){
            res &= it->ctx_array[i].completed;
        }
    }

    _scan_merge(it);

    return (it->nb_items==0) ? false : true;
}

//get the whole item data, just issue a kv_get_async command.
struct kv_item* kv_iterator_item(struct kv_iterator *it){
    assert(it!=NULL);
    if(it->nb_items==0){
        return NULL;
    }

    return it->sorted_item[it->cursor];
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
