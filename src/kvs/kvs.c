#include "kvs_internal.h"
#include "spdk/env.h"

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

void kv_rmw_async(struct kv_item *item, modify_fn m_fn, kv_cb cb_fn, void*ctx){
    _assert_parameters(item,cb_fn);
    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = _hash_shard_to_worker(shard_id);
    worker_enqueue_rmw(g_kvs->workers[worker_id],shard_id,item,m_fn,cb_fn,ctx);
}

struct _scan_worker_ctx{
    uint32_t worker_id;
    volatile bool completed;
    int kverrno;
    struct worker_scan_result* scan_res;
    struct kv_iterator *it;
};

struct sorted_item{
    uint32_t wid;
    struct kv_item* item;
};

// A little-top heap
struct kv_iterator{
    uint32_t nb_workers;
    uint32_t batch_size;
    uint32_t nb_items;

    //when issuing a seek command, no item will returned.
    //I have to store it into other place in case that the
    //user releases the item.
    struct sorted_item seek_item;

    uint64_t *per_worker_idx;
    struct _scan_worker_ctx *ctx_array;
    struct sorted_item* item_array;
};

struct kv_iterator* kv_iterator_alloc(uint32_t batch_size){
    assert(g_kvs!=NULL);
    assert(batch_size>0);

    struct kv_iterator *it;
    uint32_t size = sizeof(struct kv_iterator) + 
                    g_kvs->nb_workers * sizeof(uint64_t) +
                    g_kvs->nb_workers * sizeof(*it->ctx_array) +
                    sizeof(*it->item_array)*(g_kvs->nb_workers) +
                    MAX_SLAB_SIZE;

    it = malloc(size);
    assert(it);

    it->nb_workers = g_kvs->nb_workers;
    it->batch_size = batch_size;
    it->nb_items   = 0;
    it->per_worker_idx = (uint64_t*)(it+1);
    it->ctx_array  = (struct _scan_worker_ctx *)(it->per_worker_idx + g_kvs->nb_workers);
    it->item_array = (struct sorted_item*)(it->ctx_array + g_kvs->nb_workers);

    memset(it->per_worker_idx,0,g_kvs->nb_workers);
    it->seek_item.item = (struct kv_item*)(it->item_array + g_kvs->nb_workers);
    it->seek_item.wid  = 0;

    uint32_t i = 0;
    for(;i<g_kvs->nb_workers;i++){
        it->ctx_array[i].worker_id = i;
        it->ctx_array[i].it = it;
        it->ctx_array[i].scan_res = NULL;
    }
    return it;
}

static void
_reset_scan_res(struct _scan_worker_ctx *swctx){
    uint32_t j=0;
    if(swctx->scan_res){
        for(j=0;j<swctx->scan_res->nb_items;j++){
            free(swctx->scan_res->items[j]);
        }
        free(swctx->scan_res);
        swctx->scan_res = NULL;
        swctx->kverrno = 0;
        swctx->completed = false;
    }
    swctx->scan_res = NULL;
    swctx->it->per_worker_idx[swctx->worker_id] = 0;
}

static void
_reset_iterator(struct kv_iterator *it){
    uint32_t i=0;
    for(i=0;i<g_kvs->nb_workers;i++){
        struct _scan_worker_ctx *swctx = &it->ctx_array[i];
        _reset_scan_res(swctx);
    }
    it->nb_items  = 0;   
}

void kv_iterator_release(struct kv_iterator *it){
    assert(it!=NULL);
    _reset_iterator(it);
    free(it);
}

static inline int
_item_cmp(const struct sorted_item* item0,const struct sorted_item* item1 ){
    
    //A null item is less than any item.
    if(!item1){
        return 1;
    }
    if(!item0){
        return -1;
    }

    const uint8_t *key0 = item0->item->data;
    const uint8_t *key1 = item1->item->data;
    uint32_t len0   = item0->item->meta.ksize;
    uint32_t len1   = item1->item->meta.ksize;

    uint32_t n = len0<len1 ? len0 : len1;
    int res = memcmp(key0,key1,n);
    if(!res){
        res = len0==len1 ? 0 : (len0<len1 ? -1 : 1 );
    }
    return res;
}

static inline void
_item_swap(struct sorted_item* item0, struct sorted_item* item1){
    struct  sorted_item tmp;
    tmp=*item0;
    *item0 = *item1;
    *item1 = tmp;
}

static void 
_heap_adjust(struct sorted_item* item_array, int index, int len){

    int left=2*index+1;
    int right=2*index+2;
    
    if (left>len-1) {
        return;
    }
    else if(left==len-1){
        if(_item_cmp(&item_array[index],&item_array[left])<0){
            _item_swap(&item_array[index],&item_array[left]);
        }
        return;
    }
    else{
        int il = _item_cmp(&item_array[index],&item_array[left]);
        int ir = _item_cmp(&item_array[index],&item_array[right]);
        int lr = _item_cmp(&item_array[left],&item_array[right]);
        if (il<0 || ir<0) {
            if (lr<0) {
                //swap right with parent
                _item_swap(&item_array[index],&item_array[right]);
                _heap_adjust(item_array,len,right);
            }
            else{
                //swap left with parent
                _item_swap(&item_array[index],&item_array[left]);
                _heap_adjust(item_array,len,left);
            }
        }
    }
}

static int 
_make_heap(struct sorted_item* item_array, int len){
    int i;
    for (i=len-1; i>=0; i--) {
        if(2*i+1>len-1){
            continue;
        }
        _heap_adjust(item_array,i,len);
    }
    return 0;
}

static void
_seek_cb_fn(void*ctx, struct kv_item* item, int kverrno){
    struct _scan_worker_ctx * swctx = ctx;
    swctx->kverrno = kverrno;
    swctx->completed = true;
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

bool kv_iterator_seek(struct kv_iterator *it, struct kv_item *item){
    assert(it!=NULL);
    _assert_parameters(item,_seek_cb_fn);

    _reset_iterator(it);
    
    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = _hash_shard_to_worker(shard_id);
    worker_enqueue_seek(g_kvs->workers[worker_id],item,_seek_cb_fn,&it->ctx_array[worker_id]);

    while(!it->ctx_array[worker_id].completed){
        spdk_pause();
    }
    
    if(it->ctx_array[worker_id].kverrno){
        //seek fails;
        return false;
    }

    //copy the item into seek_item.
    //I will use it in the "next" operation. 
    memcpy(it->seek_item.item->data,item->data,item->meta.ksize);
    it->seek_item.item->meta.ksize = item->meta.ksize;
    it->seek_item.item->meta.vsize = 0;
    it->seek_item.wid = worker_id;

    it->nb_items = 1;
    it->item_array[0] = it->seek_item;
    it->per_worker_idx[worker_id] = 0;

    //pre-load items from all workers;
    uint32_t i=0;
    for(;i<it->nb_workers;i++){
        struct _scan_worker_ctx *swctx  = &it->ctx_array[i];
        swctx->completed = false;
        swctx->kverrno = 0;
        uint32_t batch_size = i==worker_id ? it->batch_size - 1 : it->batch_size;
        if(batch_size==0){
            //scan at least one item.
            batch_size = 1;
        }
        worker_enqueue_next(g_kvs->workers[i],it->seek_item.item,_iter_cb_fn,batch_size,swctx);
    }

    bool res = false;
    while(!res){
        res = true;
        for(i=0;i<g_kvs->nb_workers;i++){
            res &= it->ctx_array[i].completed;
        }
        spdk_pause();
    }

    for(i=0;i<it->nb_workers;i++){
        struct worker_scan_result *scan_res = it->ctx_array[i].scan_res;
        if( !scan_res || scan_res->nb_items==0 ){
            //No more items in this worker;
            it->per_worker_idx[i] = UINT64_MAX;
        }
        else if(i!=worker_id){
            //I have seeked the item from the worker. skip it.
            it->item_array[it->nb_items].wid = i;
            it->item_array[it->nb_items].item = scan_res->items[0];
            it->nb_items++;
            it->per_worker_idx[i]++;
        }
    }

    //All items have been loaded. Make heap;
    _make_heap(it->item_array,it->nb_items);

    //All cursors have been set.
    return true;
}

bool kv_iterator_first(struct kv_iterator *it){
    assert(it!=NULL);
    
    _reset_iterator(it);
    //set the cursor for all workers.
    uint32_t i=0;
    for(;i<it->nb_workers;i++){
        struct _scan_worker_ctx *swctx  = &it->ctx_array[i];
        swctx->completed = false;
        swctx->kverrno = 0;
        worker_enqueue_first(g_kvs->workers[i],_iter_cb_fn,it->batch_size,swctx);
    }

    bool res = false;
    while(!res){
        res = true;
        for(i=0;i<g_kvs->nb_workers;i++){
            res &= it->ctx_array[i].completed;
        }
        spdk_pause();
    }

    for(i=0;i<it->nb_workers;i++){
        struct worker_scan_result *scan_res = it->ctx_array[i].scan_res;
        if( scan_res->nb_items==0 ){
            //No more items in this worker;
            it->per_worker_idx[i] = UINT64_MAX;
        }
        else{
            //I have seeked the item from the worker. skip it.
            it->item_array[it->nb_items].wid = i;
            it->item_array[it->nb_items].item = scan_res->items[0];
            it->nb_items++;
            it->per_worker_idx[i]++;
        }
    }
    _make_heap(it->item_array,it->nb_items);
    //All cursors have been set.
    return true;
}

bool kv_iterator_next(struct kv_iterator *it){
    assert(it!=NULL);
    
    if(it->nb_items==0){
        return false;
    }

    uint32_t wid = it->item_array[0].wid;
    struct _scan_worker_ctx *swctx = &it->ctx_array[wid];

    if(it->per_worker_idx[wid]>=swctx->scan_res->nb_items){
        //No more items left in the current batch.
        if(swctx->scan_res->nb_items==swctx->scan_res->batch_size){
            //Try to pre-load more items for this worker.
            //The last item of this batch is set as the current cursor.
            struct kv_item *last_item = swctx->scan_res->items[swctx->scan_res->nb_items-1];
            memcpy(it->seek_item.item->data,last_item->data,last_item->meta.ksize);

            it->seek_item.item->meta.ksize = last_item->meta.ksize;
            it->seek_item.item->meta.vsize = 0;
            it->seek_item.wid = swctx->worker_id;
            
            //Need I scan batch_size items ??
            _reset_scan_res(swctx);
            worker_enqueue_next(g_kvs->workers[wid],it->seek_item.item,_iter_cb_fn,it->batch_size,swctx);

            //Just wait the completion.
            while(!swctx->completed){
                spdk_pause();
            }
        }
    }

    if(it->per_worker_idx[wid] < swctx->scan_res->nb_items){
        //There are items left in the batch.
        it->item_array[0].wid = swctx->worker_id;
        it->item_array[0].item = swctx->scan_res->items[it->per_worker_idx[wid]];
        it->per_worker_idx[wid]++;
        _heap_adjust(it->item_array,0,it->nb_items);
    }
    else if(it->nb_items>1){
        //No more items left for the worker.
        it->per_worker_idx[wid] = UINT64_MAX;
        it->item_array[0] = it->item_array[it->nb_items-1];
        it->nb_items--;
        _heap_adjust(it->item_array,0,it->nb_items);
    }
    else{
        //No more items left for the whole database.
        //Just tell the user that all items have been iterated.
        it->nb_items = 0;
    }

    return (it->nb_items==0) ? false : true;
}

//get the item containing only meta key info.
struct kv_item* kv_iterator_item(struct kv_iterator *it){
    assert(it!=NULL);
    if(it->nb_items==0){
        return NULL;
    }

    return it->item_array[0].item;
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
