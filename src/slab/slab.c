#include "slab.h"
#include "pagechunk.h"
#include "kvutil.h"
#include "kverrno.h"

#include "spdk/thread.h"
#include "spdk/log.h"
#include "spdk/env.h"

//All slab sizes are 4-bytes-aligment.
static const uint32_t _g_slab_chunk_pages = 16;
static uint32_t slab_sizes[]={
    32, 36, 40,48, 56, 64, 72, 88, 96, 104, 124, /* wrapped in one page */
    128, 160, 192, 224, 256, 320, 384, 480, 576, 704, 864, 1056, 1312, 1632, 2016, 2496, 3104, 3872, 4832, 6016, 7520, 9376, 11712, 14624
};


uint32_t 
slab_find_slab(uint32_t item_size){
    uint32_t len = sizeof(slab_sizes)/sizeof(slab_sizes[0]);

    //User passes an invalid item size, so the program shall crash forcely.
    assert( item_size>=slab_sizes[0] && item_size<=slab_sizes[len-1] );

    int i = 0;
    int j = len-1;
    int mid = (i+j)/2;
    while(1){
        if(slab_sizes[mid]<=item_size && slab_sizes[mid+1]>=item_size){
            return slab_sizes[mid]==item_size?mid:mid+1;
        }else if(slab_sizes[mid]>item_size){
            j = mid;
        }else{
            i = mid+1;
        }
        mid = (i+j)/2;
    }
}

void slab_get_slab_conf(uint32_t **slab_size_array, uint32_t *nb_slabs, uint32_t *chunk_pages){
    uint32_t _nb_slabs = sizeof(slab_sizes)/sizeof(slab_sizes[0]);
    *slab_size_array = slab_sizes;
    *nb_slabs = _nb_slabs;
    *chunk_pages = _g_slab_chunk_pages;
}

bool 
slab_is_slab_changed(uint32_t old_item_size, uint32_t new_item_size){
    uint32_t idx = slab_find_slab(new_item_size);
    uint32_t size = slab_sizes[idx];
    
    return old_item_size != size ? true : false;
}

bool 
slab_is_valid_size(uint32_t slab_size, uint32_t item_size){

    uint32_t len = sizeof(slab_sizes)/sizeof(slab_sizes[0]);

    uint32_t min_slab = slab_sizes[0];
    uint32_t max_slab = slab_sizes[len-1];

    if(item_size<min_slab || item_size>max_slab){
        return false;
    }

    uint32_t idx = slab_find_slab(item_size);

    return slab_sizes[idx]==slab_size ? true : false;
}

bool 
slab_is_slot_occupied(struct slab* slab,uint64_t slot_idx){
    
    struct reclaim_node* node;
    struct chunk_desc *desc;
    uint64_t slot_offset;

    pagechunk_get_hints(slab,slot_idx,&node,&desc,&slot_offset);
    assert(node!=NULL);

    return bitmap_get_bit(desc->bitmap,slot_offset) ? true : false;
}

/*
void 
slab_create_async(struct iomgr* imgr,
                       uint32_t slab_size, 
                       char* slab_name, 
                       void (*cb)(struct slab* slab, void* ctx,int kverrno),
                       void* ctx){
    //to do
}
*/

struct resize_ctx{
    struct slab* slab;
    uint64_t new_size; //chunks
    uint32_t nb_nodes;
    int kverrno;

    struct spdk_thread *thread;

    //Callback when the blob is resized. The ctx is the resize_ctx self.
    void (*resize_cb)(void*ctx);

    void (*user_slot_cb)(uint64_t slot_idx,void*ctx, int kverrno);
    void (*user_truncate_cb)(void* ctx, int kverrno);
    void* user_ctx;

    TAILQ_ENTRY(resize_ctx) link;
};

static void _slab_blob_resize_common_cb(void*ctx);

static void
_slab_blob_md_sync_complete(void*ctx, int bserrno){
    struct resize_ctx *rctx = ctx;

    if(bserrno){
        SPDK_NOTICELOG("blob md sync error:%d\n",bserrno);
    }

    rctx->kverrno =  bserrno ? -KV_EIO : -KV_ESUCCESS;
    spdk_thread_send_msg(rctx->thread,rctx->resize_cb,rctx);
}

static void
_slab_blob_resize_complete(void*ctx, int bserrno){
    struct resize_ctx *rctx = ctx;
    struct spdk_blob *blob = rctx->slab->blob;
    if(bserrno){
        //Resize error;
        SPDK_NOTICELOG("blob resize error:%d\n",bserrno);
        rctx->kverrno = -KV_EIO;
        spdk_thread_send_msg(rctx->thread,rctx->resize_cb,rctx);
        return;
    }
    spdk_blob_sync_md(blob, _slab_blob_md_sync_complete, rctx);
}

static void
_slab_blob_resize(void* ctx){
    struct resize_ctx *rctx = ctx;
    struct spdk_blob *blob = rctx->slab->blob;
    uint64_t new_size = rctx->new_size;
    spdk_blob_resize(blob,new_size,_slab_blob_resize_complete,rctx);
    //uint32_t old_size = rctx->slab->reclaim.nb_reclaim_nodes * rctx->slab->reclaim.nb_chunks_per_node;
    //SPDK_NOTICELOG("slab %p resized, slab size:%u old size:%u,new size:%lu\n",rctx->slab,rctx->slab->slab_size,old_size,rctx->new_size);
}

// static void
// _slab_truncate_resize_complete(void*ctx){
//     struct resize_ctx *rctx = ctx;
//     rctx->user_truncate_cb(rctx->user_ctx,rctx->kverrno ? -KV_EIO : -KV_ESUCCESS);
//     free(rctx);
// }

// static void
// _truncate_unmap_complete(void*ctx, int bserrno){
//     struct resize_ctx *rctx = ctx;
//     if(bserrno){
//         //Unmap failed.
//         rctx->user_truncate_cb(rctx->user_ctx,KV_EIO);
//         free(rctx);
//         return;
//     }
//     rctx->resize_cb = _slab_truncate_resize_complete;
//     rctx->thread  = spdk_get_thread();

//     spdk_thread_send_msg(rctx->thread,_slab_blob_resize,rctx);
// }

void slab_release_node_async(struct iomgr* imgr,
                       struct slab* slab,
                       struct reclaim_node* node,
                       void (*cb)(void* ctx, int kverrno),
                       void* ctx){
    
    //Only the empty node is allowed to release
    uint64_t slots_perf_node = slab->reclaim.nb_chunks_per_node * slab->reclaim.nb_slots_per_chunk;
    assert(node->nb_free_slots == slots_perf_node);

    //@TODO to be implemented
    uint64_t start_chunk = slab->reclaim.nb_chunks_per_node * node->id;
    uint64_t nb_chunks = slab->reclaim.nb_chunks_per_node;
    (void)nb_chunks;
    //spdk_blob_punch_hole(slab->blob,imgr->channel,start_chunk,nb_chunks,_punch_hole_complete,ctx);

    // uint64_t old_size = slab->reclaim.nb_reclaim_nodes * slab->reclaim.nb_chunks_per_node;
    // uint64_t new_size = old_size - nb_nodes*slab->reclaim.nb_chunks_per_node;

    // struct resize_ctx *rctx = malloc(sizeof(struct resize_ctx));
    // if(!rctx){
    //     cb(ctx,-KV_EMEM);
    //     return;
    // }

    // rctx->slab = slab;
    // rctx->new_size = new_size;
    // rctx->user_truncate_cb = cb;
    // rctx->user_ctx = ctx;
    // rctx->kverrno = 0;

    // //Tell the disk driver that the data can be deserted.
    // uint64_t io_unit_offset = new_size*slab->reclaim.nb_pages_per_chunk*imgr->io_unit_size;
    // uint64_t io_unit_length = nb_nodes*slab->reclaim.nb_chunks_per_node*slab->reclaim.nb_pages_per_chunk*imgr->io_unit_size;
    // spdk_blob_io_unmap(slab->blob,imgr->channel,io_unit_offset,io_unit_length,_truncate_unmap_complete,rctx);
}

static inline uint64_t 
_get_one_slot_from_free_slab(struct slab*slab, struct reclaim_node* node) {
    assert(slab->reclaim.nb_free_slots!=0);
    assert(node->nb_free_slots!=0);

    struct chunk_desc* desc;
    uint64_t slot_idx = UINT64_MAX;

    uint32_t i = 0;
    for(;i<slab->reclaim.nb_chunks_per_node;i++){
        if(node->desc_array[i]->nb_free_slots){
            desc = node->desc_array[i];
            uint32_t offset = bitmap_get_first_clear_bit(desc->bitmap);

            assert(offset!=UINT32_MAX);
            bitmap_set_bit(desc->bitmap,offset);

            desc->nb_free_slots--;
            node->nb_free_slots--;
            slab->reclaim.nb_free_slots--;

            if(!node->nb_free_slots){
                //Wow! The reclaim node is full. I should remove it from
                //free_node treemap.
                rbtree_delete(slab->reclaim.free_node_tree,node->id,NULL);
            }

            uint64_t base = node->id*slab->reclaim.nb_chunks_per_node*slab->reclaim.nb_slots_per_chunk + 
                            i*slab->reclaim.nb_slots_per_chunk;
            slot_idx = offset + base;
            break;
        }
    }
    return slot_idx;
}

static void
_slab_blob_resize_common_cb(void*ctx){
    struct resize_ctx* rctx = ctx;
    struct slab* slab = rctx->slab;

    uint32_t nb_nodes = rctx->nb_nodes;
    struct reclaim_node *nodes[nb_nodes];

    if(!rctx->kverrno){
        uint32_t i=0;
        for(;i<nb_nodes;i++){
            nodes[i] = slab_reclaim_alloc_one_node(slab,slab->reclaim.nb_reclaim_nodes+i);
            if(!nodes[i]){
                break;
            }
        }
        assert(i==nb_nodes && "No enough memory");
        // if(i!=nb_nodes){
        //     //Fail to allocate enough nodes memory.
        //     rctx->kverrno = -KV_EMEM;
        //     for(;i>0;i--){
        //         free(nodes[i-1]);
        //     }
        // }
    }

    if(!rctx->kverrno){
        uint32_t i=0;
        for(;i<nb_nodes;i++){
            slab->reclaim.nb_total_slots += nodes[i]->nb_free_slots;
            slab->reclaim.nb_free_slots += nodes[i]->nb_free_slots;
            slab->reclaim.nb_reclaim_nodes++;
            rbtree_insert(slab->reclaim.free_node_tree,nodes[i]->id,nodes[i],NULL);
        }
    }

    //in case of resizing
    if(slab->reclaim.nb_reclaim_nodes == slab->reclaim.cap){
        slab->reclaim.node_array = realloc(slab->reclaim.node_array,(slab->reclaim.cap*2)*sizeof(void*));
        assert(slab->reclaim.node_array);
        slab->reclaim.cap *= 2;
    }
    for(uint32_t i=0; i<nb_nodes;i++){
        slab->reclaim.node_array[slab->reclaim.nb_reclaim_nodes++] = nodes[i];
    }

    //index of the node that have free slots.
    uint32_t i = 0;
    struct resize_ctx* req,*tmp = NULL;

    TAILQ_FOREACH_SAFE(req,&slab->resize_head,link,tmp){
        TAILQ_REMOVE(&slab->resize_head,req,link);
        if(rctx->kverrno){
            req->user_slot_cb(UINT64_MAX,req->user_ctx,rctx->kverrno);
        }
        else{
            i =  nodes[i]->nb_free_slots ? i : i+1;
            uint64_t slot = _get_one_slot_from_free_slab(slab,nodes[i]);
            req->user_slot_cb(slot,req->user_ctx,-KV_ESUCCESS);
        }
        free(req);
    }
}

void slab_request_slot_async(struct iomgr* imgr,
                             struct slab* slab, 
                             void (*cb)(uint64_t slot_idx, void* ctx, int kverrno), 
                             void* ctx){
    
    if(slab->reclaim.nb_free_slots!=0){
        struct reclaim_node* node = rbtree_first(slab->reclaim.free_node_tree);
        uint64_t slot = _get_one_slot_from_free_slab(slab,node);
        
        cb(slot,ctx,-KV_ESUCCESS);
        return;
    }

    //Not free slot in the slab, now resize it.
    
    if(slab->flag&SLAB_FLAG_RECLAIMING){
        //The slab is fully utilized, but it is in reclaiming state. I am really overwhelmed.
        //This may be caused by incorrect reclaiming checking algorithm.
        assert(0);
    }

    uint32_t nb_slots_per_node = slab->reclaim.nb_slots_per_chunk * 
                                 slab->reclaim.nb_chunks_per_node;

    //The nodes number that should be resized.
    uint32_t nb_nodes = imgr->max_pending_io/nb_slots_per_node + 
                        !!(imgr->max_pending_io%nb_slots_per_node);
    
    //align to 4MB
    const uint64_t min_alloc_nodes = (4*1024*1024)/(slab->slab_size * nb_slots_per_node);
    nb_nodes = nb_nodes < min_alloc_nodes ? min_alloc_nodes : nb_nodes;

    if(spdk_bs_free_cluster_count(imgr->target) < nb_nodes){
        //No enough space for the disk.
        cb(UINT64_MAX,ctx,-KV_EFULL);
        return;
    }

    //Since it seldom happens, I use malloc here, which does not cost much.
    struct resize_ctx *rctx = malloc(sizeof(struct resize_ctx));
    if(!rctx){
        cb(UINT64_MAX,ctx,-KV_EMEM);
        return;
    }
    uint32_t old_size = slab->reclaim.nb_reclaim_nodes * slab->reclaim.nb_chunks_per_node;
    rctx->slab = slab;
    rctx->new_size = old_size + slab->reclaim.nb_chunks_per_node * nb_nodes;
    rctx->nb_nodes = nb_nodes;
    rctx->thread = spdk_get_thread();
    rctx->user_slot_cb = cb;
    rctx->user_ctx = ctx;
    rctx->kverrno = 0;

    if(!TAILQ_EMPTY(&slab->resize_head)){
        //Other request is resizing the slab;
        rctx->resize_cb = NULL;
        TAILQ_INSERT_TAIL(&slab->resize_head,rctx,link);
    }
    else{
        //This is the first resizing request.
        rctx->resize_cb = _slab_blob_resize_common_cb;
        TAILQ_INSERT_TAIL(&slab->resize_head,rctx,link);
        spdk_thread_send_msg(imgr->meta_thread,_slab_blob_resize,rctx);
    }
}

void slab_free_slot_async(struct reclaim_mgr* rmgr,
                          struct slab* slab, 
                          uint64_t slot_idx,
                          void (*cb)(void* ctx, int kverrno),
                          void* ctx){
                              
    //Ok. Just post the deleting to background reclaiming thread.
    slab_reclaim_post_delete(rmgr,slab,slot_idx,cb,ctx);
}

