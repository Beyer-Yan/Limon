#include "slab.h"
#include "pagechunk.h"
#include "kvutil.h"
#include "kverrno.h"

//All slab sizes are 4-bytes-aligment.
static const uint32_t _g_slab_chunk_pages = 252;
static uint32_t slab_sizes[]={
    32, 36, 40,48, 56, 64, 72, 88, 96, 104, 124, 144, 196, 224, 256, 272, 292, 316, 344, 372, 408, 456,
    512, 584, 684,
    768,864,960,1024,1152,1280,1440,1536,1728,1920,2048,2304,2560,2880,3072,3456,3840,4096,4320,4608,5120,
    5760,6144,6912,7680,8640,9216,10240
};

static const uint32_t _g_slab_chunk_pages_old = 252;
static uint32_t slab_sizes_old[] = { 32, 36, 40,48, 56, 64, 72, 88, 96, 104, 124, 144, 196, 224, 
                     256, 272, 292, 316, 344, 372, 408, 456,512, 584, 684, 
                     768,864,960, 1024, 1364, 2048,4096,
                     
                     4608,5376,6144,
                     7168,8192,9216,10752,12288,14336,16384,18432,21504,24576,28672,32256,36864,
                     43008,49152,57344,64512,73728,77824, 86016, 
                     
                     94208, 102400, 110592, 122880, 
                     135168, 147456, 163840, 180224,196608, 217088, 237568, 262144, 290816, 
                     319488, 352256, 389120, 430080, 475136, 524288, 581632, 643072, 712704, 
                     790528, 876544, 970752, 1077248, 1196032, 1327104, 1474560, 1638400, 1818624,
                     2019328, 2240512, 2486272, 2760704, 3063808, 3403776, 3780608, 4194304
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
    if(old_item_size != slab_find_slab(new_item_size)) {
        return true;
    }
    return false;
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

void 
slab_resize_async(struct iomgr* imgr,
                       struct slab* slab,
                       uint64_t new_size,
                       void (*cb)(void* ctx, int kverrno),
                       void* ctx){

    spdk_blob_resize(slab->blob,new_size,cb,ctx);
}

struct resize_ctx{
    struct slab* slab;
    void (*user_cb)(uint64_t slot_idx,void*ctx, int kverrno);
    void* ctx;
};

static void
_slab_md_sync_complete(void*ctx, int kverrno){
    struct resize_ctx* rctx = ctx;
    struct slab* slab = rctx->slab;

    if(kverrno){
        rctx->user_cb(-1,rctx->ctx,kverrno);
        free(rctx);
        return;
    }  

    struct reclaim_node *node = slab_reclaim_alloc_one_node(slab,slab->reclaim.nb_reclaim_nodes+1);
    if(!node){
        rctx->user_cb(-1,rctx->ctx,-KV_EMEM);
        free(rctx);
        return;
    }

    slab->reclaim.nb_free_slots += node->nb_free_slots;
    slab->reclaim.nb_total_slots += node->nb_free_slots;
    slab->reclaim.nb_reclaim_nodes++;
    rbtree_insert(slab->reclaim.total_tree,node->id,node,NULL);
    rbtree_insert(slab->reclaim.free_node_tree,node->id,node,NULL);

    //Just retrieve a slot from the first chunk of the newly allocated reclaim node;
    //The first slot of the first page chunk of the reclaim node is the one I want.
    struct chunk_desc*desc = (struct chunk_desc*)(node+1);
    uint64_t slot_idx = node->id*slab->reclaim.nb_chunks_per_node*slab->reclaim.nb_slots_per_chunk;
    bitmap_set_bit(desc->bitmap,0);
    desc->nb_free_slots--;
    node->nb_free_slots--;
    slab->reclaim.nb_free_slots--;

    rctx->user_cb(slot_idx,rctx->ctx,-KV_ESUCCESS);
    free(rctx); 
}

static void
_resize_complete_cb(void*ctx, int kverrno){
    struct resize_ctx* rctx = ctx;
    struct slab* slab = rctx->slab;

    if(kverrno){
        rctx->user_cb(-1,rctx->ctx,kverrno);
        free(rctx);
        return;
    }
    spdk_blob_sync_md(slab->blob, _slab_md_sync_complete, rctx);
}

void slab_request_slot_async(struct iomgr* imgr,
                             struct slab* slab, 
                             void (*cb)(uint64_t slot_idx, void* ctx, int kverrno), 
                             void* ctx){
    
    if(slab->reclaim.nb_free_slots!=0){
        struct reclaim_node* node = rbtree_first(slab->reclaim.free_node_tree);
        struct chunk_desc* desc;

        int i = 0;
        for(;i<slab->reclaim.nb_chunks_per_node;i++){
            if(node->desc_array[i]->nb_free_slots){
                desc = node->desc_array[i];
                uint32_t offset = bitmap_get_first_clear_bit(desc->bitmap);
                bitmap_set_bit(desc->bitmap,offset);

                desc->nb_free_slots--;
                node->nb_free_slots--;
                slab->reclaim.nb_free_slots--;

                if(!node->nb_free_slots){
                    //Wow! The reclaim node is full. I should remove it from
                    //free_node treemap.
                    rbtree_delete(slab->reclaim.free_node_tree,node->id,NULL);
                }

                uint64_t base = node->id*slab->reclaim.nb_chunks_per_node*slab->reclaim.nb_slots_per_chunk + i*slab->reclaim.nb_slots_per_chunk;
                uint64_t slot_idx = offset + base;
                cb(slot_idx,ctx,-KV_ESUCCESS);
                return;
            }
        }
    }
    
    if(slab->flag&SLAB_FLAG_RECLAIMING){
        //The slab is fully utilized, but it is in reclaiming state. I am really overwhelmed.
        //This may be caused by incorrect reclaiming checking algorithm.
        assert(0);
    }

    //The slab is fully utilized. It should be resized.
    uint64_t new_size = (slab->reclaim.nb_reclaim_nodes+1) * slab->reclaim.nb_chunks_per_node;
    
    //Since it seldom happens, I use malloc here, which does not cost much.
    struct resize_ctx *resize_ctx = malloc(sizeof(struct resize_ctx));
    resize_ctx->slab = slab;
    resize_ctx->user_cb = cb;
    resize_ctx->ctx = ctx;
    spdk_blob_resize(slab->blob,new_size,_resize_complete_cb,resize_ctx);
}

void slab_free_slot_async(struct reclaim_mgr* rmgr,
                          struct slab* slab, 
                          uint64_t slot_idx,
                          void (*cb)(void* ctx, int kverrno),
                          void* ctx){
    struct reclaim_node *node;
    struct chunk_desc *desc;

    uint64_t slot_offset;

    pagechunk_get_hints(slab,slot_idx,&node,&desc,&slot_offset);
    uint32_t node_id = slot_idx/slab->reclaim.nb_slots_per_chunk/slab->reclaim.nb_chunks_per_node;
    bitmap_clear_bit(desc->bitmap,slot_offset);
    
    if(!node->nb_free_slots){
        //This node is full node. But now, there is a empty slot for it. So I should put it
        //in free_node treemap;
        rbtree_insert(slab->reclaim.free_node_tree,node_id,node,NULL);
    }

    desc->nb_free_slots++;
    node->nb_free_slots++;
    slab->reclaim.nb_free_slots++;

    //Ok. Just post the deleting to background reclaiming thread.
    slab_reclaim_post_delete(rmgr,slab,slot_idx,cb,ctx);
}

