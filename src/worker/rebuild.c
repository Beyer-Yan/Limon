#include "worker_internal.h"
#include "slab.h"
#include "kverrno.h"
#include "spdk/env.h"
#include "spdk/queue.h"
#include "spdk/log.h"

struct list_slab{
    struct slab* slab;
    uint32_t shard_idx;
    LIST_ENTRY(list_slab) link;
};

struct rebuild_ctx{
    struct worker_context *wctx;

    //A temporary mem index for slab recovery. When the slab is rebuilt, the mem index
    //will be destroyed.
    struct mem_index *index_for_one_slab;

    LIST_HEAD(,list_slab) slab_head;
    struct list_slab* cur;
    uint32_t reclaim_node_idx;

    void(*slab_rebuild_complete)(struct rebuild_ctx *rctx,int kverrno);

    void(*cb_fn)(void*ctx, int kverrno);
    void*ctx;

    uint32_t nb_buffer_pages;
    uint8_t *recovery_node_buffer;
};

static void _rebuild_one_node_async(struct rebuild_ctx* rctx);
static void _rebuild_one_slab(struct rebuild_ctx* rctx);

static void
_node_read_complete(void* ctx, int bserrno){
    struct rebuild_ctx *rctx = ctx;
    struct worker_context *wctx = rctx->wctx;

    if(bserrno){
        rctx->slab_rebuild_complete(rctx,-KV_EIO);
        return;
    }

    struct slab *slab = rctx->cur->slab;

    struct reclaim_node* node = slab_reclaim_alloc_one_node(slab,rctx->reclaim_node_idx);
    assert(node!=NULL);

    uint64_t slot_base = rctx->reclaim_node_idx*slab->reclaim.nb_chunks_per_node*
                          slab->reclaim.nb_slots_per_chunk ;
    uint32_t idx = 0;
    uint32_t nb_slots_per_node = slab->reclaim.nb_chunks_per_node * slab->reclaim.nb_slots_per_chunk;
    for(;idx<nb_slots_per_node;idx++){
        uint8_t *raw_data = rctx->recovery_node_buffer + slab_slot_offset(slab,idx);
        uint64_t tsc0;
        struct kv_item *item = (struct kv_item*)(raw_data + 8);
        uint64_t tsc1;

        memcpy(&tsc0,raw_data,sizeof(tsc0));
        memcpy(&tsc1,raw_data + item_get_size(item) + 8,sizeof(tsc1));

        if(item->meta.ksize!=0){
            //It may be a valid item.
            if( (tsc0!=tsc1) || (tsc0==UINT64_MAX) || (tsc0==0) ){
                //The item is crashed, which may be caused by a system crash when the item is been storing.
                //User may format a disk by 1 or 0. 
                continue;
            }
            uint32_t actual_item_size = item_get_size(item) + 16;
            if( !slab_is_valid_size(slab->slab_size,actual_item_size) ){
                //The item is invalid
                SPDK_ERRLOG("Invalid item found in slab rebuilding, shard:%u,slab size:%u, item_size:%u\n",
                            rctx->cur->shard_idx,slab->slab_size, actual_item_size);
                continue;
            }

            struct chunk_desc *desc = node->desc_array[idx/slab->reclaim.nb_slots_per_chunk];
            struct index_entry* entry_slab   = mem_index_lookup(rctx->index_for_one_slab,item);
            struct index_entry* entry_worker = mem_index_lookup(wctx->mem_index,item);
            if(entry_slab){
                assert(entry_worker!=NULL);
                //The item has been built, but I find another one because of a system crash when
                //the item is updated not-in-place and it's original slot is being reclaimed.
                //I should compare the timestamp to decide which one I should rebuild.
                //The chunk_desc field is used as the timestamp field for reducing memory usage.
                uint64_t tsmp = (uint64_t)entry_slab->chunk_desc;
                if(tsmp<tsc0){
                    //This item is newer.
                    bitmap_clear_bit(entry_worker->chunk_desc->bitmap,
                                     entry_worker->slot_idx%entry_worker->chunk_desc->nb_slots);
                    bitmap_set_bit(desc->bitmap,idx%desc->nb_slots);

                    entry_slab->chunk_desc = (struct chunk_desc *)tsc0;
                    entry_worker->slot_idx = slot_base + idx;
                    entry_worker->chunk_desc = desc;
                }
            }
            else{
                //This is a new item to be built.
                //I use the stack variable here because the entry will be copied.
                struct index_entry entry = {0};
                entry.chunk_desc = desc;
                entry.slot_idx = slot_base + idx;
                mem_index_add(wctx->mem_index,item,&entry);

                entry.chunk_desc = (struct chunk_desc *)tsc0;
                mem_index_add(rctx->index_for_one_slab,item,&entry);
                
                bitmap_set_bit(desc->bitmap,idx%desc->nb_slots);
                desc->nb_free_slots--;
                node->nb_free_slots--;
                slab->reclaim.nb_free_slots--;
            }
        }
    }

    rbtree_insert(slab->reclaim.total_tree,node->id,node,NULL);
    if(node->nb_free_slots){
        rbtree_insert(slab->reclaim.free_node_tree,node->id,node,NULL);
    }

    if(rctx->reclaim_node_idx==slab->reclaim.nb_reclaim_nodes-1){
        //All reclaim nodes are rebuilt.
        rctx->slab_rebuild_complete(rctx,-KV_ESUCCESS);
    }
    else{
        rctx->reclaim_node_idx++;
        _rebuild_one_node_async(rctx);
    }
}

static void
_rebuild_one_node_async(struct rebuild_ctx* rctx){

    struct slab *slab = rctx->cur->slab;
    uint64_t offset = rctx->reclaim_node_idx * slab->reclaim.nb_chunks_per_node * 
                      slab->reclaim.nb_pages_per_chunk;
    uint64_t pages = slab->reclaim.nb_chunks_per_node * slab->reclaim.nb_pages_per_chunk;

    spdk_blob_io_read(slab->blob,rctx->wctx->imgr->channel,rctx->recovery_node_buffer,
                      offset,pages,
                      _node_read_complete,rctx);
}

static void
_rebuild_exit(struct rebuild_ctx* rctx,int kverrno){
    if(rctx->recovery_node_buffer){
        spdk_free(rctx->recovery_node_buffer);
    }
    if(rctx->index_for_one_slab){
        mem_index_destroy(rctx->index_for_one_slab);
    }

    struct list_slab* s,*tmp;
    LIST_FOREACH_SAFE(s,&rctx->slab_head,link,tmp){
        LIST_REMOVE(s,link);
        free(s);
    }
    
    rctx->cb_fn(rctx->ctx,kverrno);
    free(rctx);
}

static void
_slab_rebuild_complete(struct rebuild_ctx* rctx,int kverrno){
    if(kverrno){
        _rebuild_exit(rctx,kverrno);
        return;
    }
    struct list_slab* s = LIST_NEXT(rctx->cur,link);
    if(!s){
        //All slabs have been rebuilt
        _rebuild_exit(rctx,-KV_ESUCCESS);
        return;
    }
    //I should continue the next slab;
    if(s->shard_idx!=rctx->cur->shard_idx){
        //I am going to rebuld the next shard.
        //And now, I should destroy the mem index for reducing memory usage.
        //As the item will never be stored across shard.
        mem_index_destroy(rctx->index_for_one_slab);
        rctx->index_for_one_slab = mem_index_init();
    }
    rctx->cur = s;
    _rebuild_one_slab(rctx);
}

static void
_rebuild_one_slab(struct rebuild_ctx* rctx){
    struct slab* slab  = rctx->cur->slab;
    
    SPDK_NOTICELOG("Rebuilding worker:%u, shard:%u, slab:%u\n", spdk_env_get_current_core(),rctx->cur->shard_idx,slab->slab_size);

    rctx->reclaim_node_idx = 0;
    //Assume that All slots are free.
    slab->reclaim.nb_free_slots = slab->reclaim.nb_total_slots;

    uint32_t nb_pages = slab->reclaim.nb_chunks_per_node * 
                        slab->reclaim.nb_pages_per_chunk;
    if(nb_pages > rctx->nb_buffer_pages){
        rctx->recovery_node_buffer = spdk_realloc(rctx->recovery_node_buffer,KVS_PAGE_SIZE*nb_pages,0x1000);
        rctx->nb_buffer_pages = nb_pages;
    }
    _rebuild_one_node_async(rctx);
}

void worker_perform_rebuild_async(struct worker_context *wctx, void(*complete_cb)(void*ctx, int kverrno),void*ctx){
    struct rebuild_ctx* rctx = malloc(sizeof(struct rebuild_ctx));
    rctx->cb_fn = complete_cb;
    rctx->ctx = ctx;
    rctx->nb_buffer_pages=0;
    rctx->wctx = wctx;
    rctx->index_for_one_slab = NULL;
    rctx->recovery_node_buffer = NULL;
    rctx->slab_rebuild_complete = _slab_rebuild_complete;
    LIST_INIT(&rctx->slab_head);

    uint32_t i = wctx->reclaim_shards_start_id;
    uint32_t j = 0;
    uint32_t nb_rebuild_shards = rctx->wctx->reclaim_shards_start_id+wctx->nb_reclaim_shards;
    for(;i<nb_rebuild_shards;i++){
        struct slab_shard *shard = &wctx->shards[i];
        for(;j<shard->nb_slabs;j++){
            struct slab* slab = &shard->slab_set[j];
            if(spdk_blob_get_num_clusters(slab->blob)){
                //This slab has valid data to be rebuilt.
                struct list_slab *s = malloc(sizeof(struct list_slab));
                assert(s!=NULL);
                s->slab = &shard->slab_set[j];
                s->shard_idx = i;
                LIST_INSERT_HEAD(&rctx->slab_head,s,link);
            }
        }
    }

    if(LIST_EMPTY(&rctx->slab_head)){
        //Nothing to be rebuilt
        _rebuild_exit(rctx,-KV_ESUCCESS);
        return;
    }

    rctx->cur = LIST_FIRST(&rctx->slab_head);
    rctx->index_for_one_slab = mem_index_init();
    rctx->nb_buffer_pages = rctx->cur->slab->reclaim.nb_chunks_per_node * 
                            rctx->cur->slab->reclaim.nb_pages_per_chunk;

    uint32_t socket_id = spdk_env_get_socket_id(spdk_env_get_current_core());
    rctx->recovery_node_buffer = spdk_malloc(rctx->nb_buffer_pages*KVS_PAGE_SIZE,0x1000,NULL,socket_id,SPDK_MALLOC_DMA);
    
    assert(rctx->recovery_node_buffer!=NULL);
    assert(rctx->index_for_one_slab);
    
    _rebuild_one_slab(rctx);
}
