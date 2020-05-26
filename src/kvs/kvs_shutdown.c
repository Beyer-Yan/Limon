#include <stdatomic.h>
#include "kvs_internal.h"

#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/blob.h"
#include "spdk/log.h"

static void
_kvs_shutdown_errno(int bserrno){
    spdk_app_stop(bserrno);
}

static void
_free_reclaim_node(rbtree_node node){
    struct reclaim_node *rnode =  node->value;
    free(rnode);
}

static void
_kvs_shutdown_bs_unload_complete(void*ctx ,int bserrno){
    if(bserrno){
        SPDK_ERRLOG("Blobstore unload failed\n");
        _kvs_shutdown_errno(bserrno);
        return;
    }
    free(g_kvs);
    g_kvs = NULL;
    spdk_app_stop(0);
}

static void
_kvs_shutdown_super_blob_close_complete(void*ctx, int bserrno){
    if(bserrno){
        SPDK_ERRLOG("close super blob failed\n");
        _kvs_shutdown_errno(bserrno);
        return;
    }

    //Release all the slab reclaim node.
    uint32_t i=0, j=0;
    for(;i<g_kvs->nb_shards;i++){
        for(j=0;j<g_kvs->shards[i].nb_slabs;j++){
            struct slab *slab = &g_kvs->shards[i].slab_set[j];
            //free the memory all the reclaim nodes
            rbtree_apply(slab->reclaim.total_tree,_free_reclaim_node);
            rbtree_destroy(slab->reclaim.total_tree);
            rbtree_destroy(slab->reclaim.free_node_tree);
        }
    }

    //All reclaim  node have been released.
    spdk_put_io_channel(g_kvs->meta_channel);
    spdk_bs_unload(g_kvs->bs_target,_kvs_shutdown_bs_unload_complete,NULL);
}

static void
_kvs_start_close_blob_next(void*ctx,int bserrno){
    struct slab* slab = ctx;
    if(bserrno){
        SPDK_ERRLOG("close blob failed, slab:%p, size:%u\n",slab,slab->slab_size);
        _kvs_shutdown_errno(bserrno);
        return;
    }
    uint32_t last_shard_idx = g_kvs->nb_shards-1;
    uint32_t last_slab_idx = g_kvs->shards[last_shard_idx].nb_slabs;
    struct slab* last_slab = &g_kvs->shards[last_shard_idx].slab_set[last_slab_idx-1];

    if(slab==last_slab){
        //This is the last slab
        //All slab blob have been closed
        //close the super blob.
        spdk_blob_close(g_kvs->super_blob,_kvs_shutdown_super_blob_close_complete,NULL);
    }
    else{
        //Since the slabs in the kvs context are  stored sequencially
        slab++;
        spdk_blob_close(slab->blob,_kvs_start_close_blob_next,slab);
    }
}

static void
_close_all_blobs(void*ctx){
    struct slab* slab = ctx;
    spdk_blob_close(slab->blob,_kvs_start_close_blob_next,slab);
}

static void
_kvs_start_close_all_blobs(void){
    //I have to send this operaiton to meta thread.
    struct slab* slab = &g_kvs->shards[0].slab_set[0];
    spdk_thread_send_msg(g_kvs->meta_thread,_close_all_blobs,slab);
    
}

static void
_kvs_shutdown_all_worker(void){
    uint32_t i=0;
    for(;i<g_kvs->nb_workers;i++){
        worker_destroy(g_kvs->workers[i]);
    }
    chunkmgr_worker_destroy();
}

void
kvs_shutdown(void){
    //Close all the slab blob, and unload the blobstore.
    SPDK_NOTICELOG("Shutdowning kvs:%s\n",g_kvs->kvs_name);
    _kvs_shutdown_all_worker();
    _kvs_start_close_all_blobs();
}
