#include "kvs_internal.h"
#include "spdk/env.h"
#include "mtable.h"

static inline uint32_t
_hash_item_to_shard(struct kv_item *item){
    //uint64_t prefix = *(uint64_t*)item->data;
    //prefix =  (((uint64_t)htonl(prefix))<<32) + htonl(prefix>>32); 
    //return prefix%g_kvs->nb_shards;
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

//static volatile uint64_t times = 0;

// The key field of item shall be filed
void kv_get_async(struct kv_item *item, kv_cb cb_fn, void* ctx){
    _assert_parameters(item,cb_fn);

    uint64_t sid = mem_index_lookup(g_kvs->global_index,item);
    if(!sid){
        //no such item
        cb_fn(ctx,NULL,-KV_EITEM_NOT_EXIST);
    }
    else{
        //worker starts from 1, worker 0 is reserved for meta-worker
        uint32_t wid = mtable_get_worker_from_sid(sid) - 1;
        worker_enqueue_get(g_kvs->workers[wid],item,sid,cb_fn,ctx);
    }

    // times++;
    // //for workload skew simulation

    // uint64_t r = *(uint64_t*)(item->data+8);
    // bool filter = false;

    // if( times > 20000000){
    //     if(worker_id==0){
    //         filter = r%10<9;
    //     }else if(worker_id==1){
    //         filter = r%10<9;
    //     }
    //     else if(worker_id==2){
    //         filter = r%10<9;
    //     }
    //     else if(worker_id==3){
    //         filter = false;
    //     }
    // }

    // if(filter){
    //     free(item);
    // }
    // else{
    //     worker_enqueue_get(g_kvs->workers[worker_id],shard_id,item,cb_fn,ctx);
    // }
}

void kv_get_with_sid_async(uint64_t sid, kv_cb cb_fn, void* ctx){
    assert(sid);
    //worker starts from 1, worker 0 is reserved for meta-worker
    uint32_t wid = mtable_get_worker_from_sid(sid) - 1;
    worker_enqueue_get(g_kvs->workers[wid],NULL,sid,cb_fn,ctx);
}

// The whole item shall be filled
void kv_put_async(struct kv_item *item, kv_cb cb_fn, void* ctx){
    _assert_parameters(item,cb_fn);

    uint64_t sid = mem_index_lookup(g_kvs->global_index,item);
    uint32_t shard_id = _hash_item_to_shard(item);
    uint32_t worker_id = _hash_shard_to_worker(shard_id);
    worker_enqueue_put(g_kvs->workers[worker_id],shard_id,item,sid,cb_fn,ctx);
}

// The key field of item shall be filed
void kv_delete_async(struct kv_item *item, kv_cb cb_fn, void* ctx){
    _assert_parameters(item,cb_fn);

    uint64_t sid = mem_index_lookup(g_kvs->global_index,item);
    if(!sid){
        //no such item
        cb_fn(ctx,NULL,-KV_EITEM_NOT_EXIST);
    }
    else{
        uint32_t shard_id = _hash_item_to_shard(item);
        uint32_t worker_id = _hash_shard_to_worker(shard_id);
        worker_enqueue_delete(g_kvs->workers[worker_id],item,sid,cb_fn,ctx);
    }
}

uint64_t kv_scan(struct kv_item *item, uint64_t maxLen,uint64_t* sid_array){
    int founds = 0;
    mem_index_scan(g_kvs->global_index,item,maxLen,&founds,sid_array);
    return founds;
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
