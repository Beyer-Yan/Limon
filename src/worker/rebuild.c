#include "worker_internal.h"
#include "slab.h"
#include "kverrno.h"
#include "spdk/env.h"
#include "spdk/queue.h"
#include "spdk/log.h"

#define INIT_NODE_CAP 256

struct list_slab{
    struct slab* slab;
    uint32_t shard_idx;
    LIST_ENTRY(list_slab) link;
};

struct rebuild_ctx{
    struct worker_context *wctx;

    //A temporary mem index for shard recovery. When the shard is rebuilt, the mem index
    //will be destroyed. The index_entry is the timestamp.
    map_t hash_tsmp;

    LIST_HEAD(,list_slab) slab_head;
    struct list_slab* cur;
    uint32_t reclaim_node_idx;

    //Used for callback when a slab is rebuilt.
    void(*slab_rebuild_complete)(struct rebuild_ctx *rctx,int kverrno);

    //User callback
    void(*cb_fn)(void*ctx, int kverrno);
    void*ctx;

    uint32_t nb_buffer_pages;
    uint8_t *recovery_node_buffer;
};


static void _read_pages(struct spdk_blob *blob, uint64_t io_unit_size, struct spdk_io_channel *channel,
		       void *payload, uint64_t offset, uint64_t length,
		       spdk_blob_op_complete cb_fn, void *cb_arg){

    uint64_t io_unit_per_page = KVS_PAGE_SIZE/io_unit_size;
    uint64_t io_unit_offset = offset*io_unit_per_page;
    uint64_t io_uint_length = length*io_unit_per_page;

    //SPDK_NOTICELOG("read pages, off:%lu,pages:%lu, lba_off:%lu,lbs_len:%lu\n",offset,length,io_unit_offset,io_uint_length);

    spdk_blob_io_read(blob,channel,payload,io_unit_offset,io_uint_length,cb_fn,cb_arg);
}

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

    //@TODO may be a hole node.

    struct slab *slab = rctx->cur->slab;
    struct reclaim_node* node = slab_reclaim_alloc_one_node(slab,rctx->reclaim_node_idx);
    assert(node!=NULL);

    assert(rctx->reclaim_node_idx<slab->reclaim.nb_reclaim_nodes);
    slab->reclaim.node_array[rctx->reclaim_node_idx] = node;

    uint64_t slot_base = rctx->reclaim_node_idx*slab->reclaim.nb_chunks_per_node*
                          slab->reclaim.nb_slots_per_chunk;
    uint32_t idx = 0;
    uint32_t nb_slots_per_node = slab->reclaim.nb_chunks_per_node * slab->reclaim.nb_slots_per_chunk;
    for(;idx<nb_slots_per_node;idx++){
        uint8_t *raw_data = rctx->recovery_node_buffer + slab_slot_offset_of_node(slab,idx);
        struct kv_item *item = (struct kv_item*)(raw_data + 8);

        if(item->meta.ksize!=0){
            //It may be a valid item.
            uint32_t actual_item_size = item_packed_size(item);
            if( !slab_is_valid_size(slab->slab_size,actual_item_size) ){
                //The item is invalid
                //SPDK_ERRLOG("Invalid item found in slab rebuilding, shard:%u,slab size:%u,slot:%lu, item_size:%u\n",
                //            rctx->cur->shard_idx,slab->slab_size,idx+slot_base ,actual_item_size);
                continue;
            }

            uint64_t tsc0, tsc1;
            memcpy(&tsc0,raw_data,sizeof(tsc0));
            memcpy(&tsc1,raw_data + item_get_size(item) + 8,sizeof(tsc1));
            //It may not be a correct item.
            if( (tsc0!=tsc1) || (tsc0==UINT64_MAX) || (tsc0==0) ){
                //The item is crashed, which may be caused by a system crash when the item is being stored.
                //User may format a disk by 1 or 0. 
                if(tsc0!=tsc1){
                    SPDK_NOTICELOG("Find broken item, drop it. slab:%u,slot:%lu, tsc0:%lu, tsc1:%lu\n",
                                    slab->slab_size,idx+slot_base,tsc0,tsc1);
                }
                continue;
            }

            //It is a valid slot
            struct chunk_desc *desc = node->desc_array[idx/slab->reclaim.nb_slots_per_chunk];
            uint64_t sid = mem_index_lookup(wctx->global_index,item);
            
            if(sid){
                //The item has been built, but I find another one because of a system crash when
                //the item is updated not-in-place and it's original slot is being reclaimed.
                //I should compare the timestamp to decide which one I should rebuild.
                uint64_t old_tsmp = 0;
                hashmap_get(rctx->hash_tsmp,sid,&old_tsmp);
                struct slot_entry* entry = mtable_get(wctx->mtable,sid);
                assert(entry);

                if(old_tsmp<tsc0){
                    //This item is newer.
                    struct slab* old_slab = &(wctx->shards[entry->shard].slab_set[entry->slab]);
                    struct reclaim_node* old_node = NULL;
                    struct chunk_desc *old_desc   = NULL;
                    uint64_t old_slot_off         = 0;

                    pagechunk_get_hints(old_slab,entry->slot_idx,&old_node,&old_desc,&old_slot_off);

                    if(old_node->nb_free_slots==0){
                        //Since we have empty slot now.
                        rbtree_insert(old_slab->reclaim.free_node_tree,old_node->id,old_node,NULL);
                    }
                    old_slab->reclaim.nb_free_slots++;
                    old_node->nb_free_slots++;
                    old_desc->nb_free_slots++;
                    bitmap_clear_bit(old_desc->bitmap,old_slot_off);

                    SPDK_NOTICELOG("Find newer item,slab:%u,slot:%lu, tsc:%lu ,ori_slab:%u,ori_slot:%lu, ori_tsc:%lu\n",
                                    slab->slab_size,idx+slot_base,tsc0,old_slab->slab_size,entry->slot_idx,old_tsmp);
                    //process the newer item
                    desc->nb_free_slots--;
                    node->nb_free_slots--;
                    slab->reclaim.nb_free_slots--;
                    bitmap_set_bit(desc->bitmap,idx%slab->reclaim.nb_slots_per_chunk);

                    //replace the slot entry;
                    entry->slab = slab_find_slab(slab->slab_size);
                    entry->slot_idx = slot_base + idx;
                    hashmap_replace(rctx->hash_tsmp,sid,(any_t)old_tsmp,(any_t)tsc0);
                }
            }
            else{
                //This is a new valid item to be built.
                struct slot_entry new_entry = {0};
                new_entry.shard = rctx->cur->shard_idx;
                new_entry.slab = slab_find_slab(slab->slab_size);
                new_entry.slot_idx = slot_base + idx;;
                
                uint64_t new_sid = mtable_alloc_sid(wctx->mtable,new_entry);
                mem_index_add(wctx->global_index,item,new_sid);
                hashmap_put(rctx->hash_tsmp,new_sid,tsc0);

                desc->nb_free_slots--;
                node->nb_free_slots--;
                slab->reclaim.nb_free_slots--;
                bitmap_set_bit(desc->bitmap,idx%slab->reclaim.nb_slots_per_chunk);
            }
        }
    }

    if(node->nb_free_slots){
        rbtree_insert(slab->reclaim.free_node_tree,node->id,node,NULL);
    }

    //SPDK_NOTICELOG("rebuild one node, shard:%u,slab:%u,node:%u\n",rctx->cur->shard_idx,slab->slab_size,rctx->reclaim_node_idx);

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

    _read_pages(slab->blob,rctx->wctx->imgr->io_unit_size,rctx->wctx->imgr->channel,rctx->recovery_node_buffer,
                offset,pages,
                _node_read_complete,rctx);
}

static void
_rebuild_exit(struct rebuild_ctx* rctx,int kverrno){
    if(rctx->recovery_node_buffer){
        spdk_free(rctx->recovery_node_buffer);
    }
    if(rctx->hash_tsmp){
        hashmap_free(rctx->hash_tsmp);
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
    //I should continue the next shard;
    if(s->shard_idx!=rctx->cur->shard_idx){
        //I am going to rebuld the next shard.
        //And now, I should destroy the mem index for reducing memory usage.
        //As the item will never be stored across shard.
        SPDK_NOTICELOG("Rebuild completes, worker:%u, shard:%u\n", spdk_env_get_current_core(),s->shard_idx);

        hashmap_free(rctx->hash_tsmp);
        rctx->hash_tsmp = hashmap_new();
    }
    rctx->cur = s;
    _rebuild_one_slab(rctx);
}

static void
_rebuild_one_slab(struct rebuild_ctx* rctx){
    struct slab* slab  = rctx->cur->slab;
    
    //SPDK_NOTICELOG("Rebuilding worker:%u, shard:%u, slab:%u, nodes:%u\n", spdk_env_get_current_core(),rctx->cur->shard_idx,slab->slab_size,slab->reclaim.nb_reclaim_nodes);

    rctx->reclaim_node_idx = 0;
    //Assume that All slots are free.
    slab->reclaim.nb_free_slots = slab->reclaim.nb_total_slots;

    uint32_t cap = slab->reclaim.nb_reclaim_nodes > INIT_NODE_CAP ?
                         slab->reclaim.nb_reclaim_nodes :
                         INIT_NODE_CAP;
    slab->reclaim.cap = cap;
    slab->reclaim.node_array = calloc(slab->reclaim.cap,sizeof(void*));
    assert(slab->reclaim.node_array);

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
    rctx->hash_tsmp = NULL;
    rctx->recovery_node_buffer = NULL;
    rctx->slab_rebuild_complete = _slab_rebuild_complete;
    LIST_INIT(&rctx->slab_head);

    uint32_t i = wctx->reclaim_shards_start_id;
    uint32_t nb_rebuild_shards = rctx->wctx->reclaim_shards_start_id+wctx->nb_reclaim_shards;
    for(;i<nb_rebuild_shards;i++){
        struct slab_shard *shard = &wctx->shards[i];
        uint32_t j = 0;
        for(j=0;j<shard->nb_slabs;j++){
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
    rctx->hash_tsmp= hashmap_new();
    rctx->nb_buffer_pages = rctx->cur->slab->reclaim.nb_chunks_per_node * 
                            rctx->cur->slab->reclaim.nb_pages_per_chunk;

    uint32_t socket_id = spdk_env_get_socket_id(spdk_env_get_current_core());
    rctx->recovery_node_buffer = spdk_malloc(rctx->nb_buffer_pages*KVS_PAGE_SIZE,0x1000,NULL,socket_id,SPDK_MALLOC_DMA);
    
    assert(rctx->recovery_node_buffer!=NULL);
    assert(rctx->hash_tsmp);
    
    _rebuild_one_slab(rctx);
}
