#include <stdatomic.h>
#include "kvs_internal.h"

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/blob_bdev.h"
#include "spdk/blob.h"
#include "spdk/log.h"
#include "spdk/string.h"

static atomic_int g_started  = 0;
struct kvs *g_kvs = NULL;

struct kvs_start_ctx{
    struct spdk_blob_store *bs;
    struct spdk_io_channel *channel;
    spdk_blob_id super_blob_id;
	struct spdk_blob *super_blob;
	uint64_t io_unit_size;
	int rc;

    struct kvs_start_opts *opts;

    uint32_t super_size;
    struct super_layout *sl;
    struct kvs *kvs;

    uint32_t *slab_size_array;
    uint32_t nb_slabs;
};

struct _blob_iter{
    struct kvs_start_ctx *kctx;
    uint32_t slab_idx;
    uint32_t total_slabs;
    struct super_layout *sl;
};

static void
_unload_complete(void *ctx, int bserrno)
{
	if (bserrno) {
		SPDK_ERRLOG("Error %d unloading the bobstore\n", bserrno);
	}

	spdk_app_stop(bserrno);
}

static void
_unload_bs(struct kvs_start_ctx *kctx, char *msg, int bserrno)
{
    if(!kctx){
        spdk_app_stop(bserrno);
        return;
    }
	if (bserrno) {
		SPDK_ERRLOG("%s (err %d)\n", msg, bserrno);
	}
	if (kctx->bs) {
		if (kctx->channel) {
			spdk_bs_free_io_channel(kctx->channel);
		}
		spdk_bs_unload(kctx->bs, _unload_complete, kctx);
	} else {
		spdk_app_stop(bserrno);
	}
    if(kctx->sl){
        spdk_free(kctx->sl);
        free(kctx);
    }
}

static void
_kvs_worker_init(struct kvs_start_ctx *kctx){

    struct chunkmgr_worker_init_opts chunk_opts;
    struct kvs* kvs = kctx->kvs;
    struct worker_context** wctx = kvs->workers;

    chunk_opts.nb_business_workers = kctx->opts->nb_works;
    chunk_opts.wctx_array = wctx;
    chunk_opts.nb_pages_per_chunk = kctx->sl->nb_pages_per_chunk;
    chunk_opts.nb_max_cache_chunks = kctx->opts->max_cache_chunks;
    chunk_opts.core_id  = kctx->opts->nb_works;
    kctx->kvs->chunkmgr_worker = chunkmgr_worker_init(&chunk_opts);

    struct worker_init_opts worker_opts;
    
    worker_opts.chunkmgr_worker = kctx->kvs->chunkmgr_worker;
    worker_opts.max_io_pending_queue_size_per_worker = kvs->max_io_pending_queue_size_per_worker;
    worker_opts.max_request_queue_size_per_worker = kvs->max_request_queue_size_per_worker;
    worker_opts.nb_reclaim_shards = kvs->nb_shards/kvs->nb_workers;
    worker_opts.nb_shards = kvs->nb_shards;
    worker_opts.reclaim_batch_size = kvs->reclaim_batch_size;
    worker_opts.reclaim_percentage_threshold = kvs->reclaim_percentage_threshold;
    worker_opts.shard = kvs->shards;
    worker_opts.target = kctx->bs;

    uint32_t i = 0;
    for(;i<kctx->opts->nb_works;i++){
        worker_opts.reclaim_shard_start_id = i*kvs->nb_workers;
        worker_opts.core_id = i;
        wctx[i] = worker_init(&worker_opts);
    }
    
    chunkmgr_worker_start();
    for(i=0;i<kctx->opts->nb_works;i++){
        worker_start(wctx[i]);
    }
    //Now all worker have been started
    g_kvs = kvs;
    spdk_free(kctx->sl);

    void (*startup_fn)(void*ctx, int kverrno) = kctx->opts->startup_fn;
    void *startup_ctx  = kctx->opts->startup_ctx;
    free(kctx);

    startup_fn(startup_ctx,-KV_ESUCCESS);
}

static void
_kvs_start_create_kvs_runtime(struct kvs_start_ctx *kctx){
    uint32_t nb_workers = kctx->opts->nb_works;
    uint32_t nb_shards  = kctx->sl->nb_shards;
    uint32_t nb_slabs_per_shard = kctx->sl->nb_slabs_per_shard;

    uint32_t size = sizeof(struct kvs) + nb_workers*sizeof(struct worker_context*) +
                    nb_shards*sizeof(struct slab_shard) +
                    nb_shards*nb_slabs_per_shard*sizeof(struct slab);
    struct kvs *kvs = malloc(size);
    assert(kvs!=NULL);

    kvs->kvs_name = kctx->opts->kvs_name;
    kvs->max_cache_chunks = kctx->opts->max_cache_chunks;
    kvs->max_key_length = kctx->sl->max_key_length;
    kvs->nb_workers = nb_workers;
    kvs->nb_shards = nb_shards;
    kvs->reclaim_batch_size = kctx->opts->reclaim_batch_size;
    kvs->reclaim_percentage_threshold = kctx->opts->reclaim_percentage_threshold;
    kvs->max_request_queue_size_per_worker = kctx->opts->max_request_queue_size_per_worker;
    kvs->max_io_pending_queue_size_per_worker = kctx->opts->max_io_pending_queue_size_per_worker;

    kvs->workers = (struct worker_context**)(kvs+1);
    kvs->shards = (struct slab_shard*)(kvs->workers + nb_workers);

    uint32_t i = 0;
    struct slab* slab_base = (struct slab*)(kvs->shards + nb_shards);
    for(;i<nb_shards;i++){
        kvs->shards[i].nb_slabs = nb_slabs_per_shard;
        kvs->shards[i].slab_set = slab_base + i*nb_slabs_per_shard;
    }
    uint32_t total_slabs = nb_shards * nb_slabs_per_shard;

    for(i=0;i<total_slabs;i++){
        slab_base[i].blob = (struct spdk_blob*)kctx->sl->slab[i].resv;
        slab_base[i].flag = 0;
        slab_base[i].slab_size = kctx->sl->slab[i].slab_size;
        slab_base[i].reclaim.nb_chunks_per_node = kctx->sl->nb_chunks_per_reclaim_node;
        slab_base[i].reclaim.nb_pages_per_chunk = kctx->sl->nb_pages_per_chunk;
        slab_base[i].reclaim.nb_slots_per_chunk = slab_get_chunk_slots(kctx->sl->nb_pages_per_chunk, 
                                                                      slab_base[i].slab_size);
        
        uint64_t slab_chunks = spdk_blob_get_num_clusters(slab_base[i].blob);
        assert(slab_chunks%slab_base[i].reclaim.nb_chunks_per_node==0);
        slab_base[i].reclaim.nb_reclaim_nodes = slab_chunks/slab_base[i].reclaim.nb_chunks_per_node;
        slab_base[i].reclaim.nb_total_slots = slab_base[i].reclaim.nb_slots_per_chunk * slab_chunks;
        slab_base[i].reclaim.total_tree = rbtree_create();
        slab_base[i].reclaim.free_node_tree = rbtree_create();
    }
    kctx->kvs = kvs;
    _kvs_worker_init(kctx);
}

static void
_kvs_start_open_blob_next(void*ctx, struct spdk_blob* blob, int bserrno){
    struct _blob_iter *iter = ctx;
    struct kvs_start_ctx *kctx = iter->kctx;

    if (bserrno) {
        free(iter);
        _unload_bs(kctx, "Error in opening blob", bserrno);
        return;
    }

    struct slab_layout *slab_base = &iter->sl->slab[iter->slab_idx];
    slab_base->resv = (uint64_t)blob;

    if(iter->slab_idx == iter->total_slabs - 1){
        //All slab have been opened;
        free(iter);
        _kvs_start_create_kvs_runtime(kctx);
    }
    else{
        iter->slab_idx++;
        slab_base = &iter->sl->slab[iter->slab_idx];
        spdk_bs_open_blob(kctx->bs,slab_base->blob_id,_kvs_start_open_blob_next,iter);
    }    
}

static void
_kvs_start_open_all_blobs(struct kvs_start_ctx *kctx){
    struct _blob_iter *iter = malloc(sizeof( struct _blob_iter));
    assert(iter!=NULL);

    iter->kctx = kctx;
    iter->total_slabs = kctx->sl->nb_shards * kctx->sl->nb_slabs_per_shard;
    iter->slab_idx = 0;
    iter->sl = kctx->sl;

    struct slab_layout *slab_base =  &iter->sl->slab[0];
    spdk_bs_open_blob(kctx->bs,slab_base->blob_id,_kvs_start_open_blob_next,iter);
}

static void
_blob_read_all_super_pages_complete(void* ctx, int bserrno){
    struct kvs_start_ctx *kctx = ctx;
    if (bserrno) {
		_unload_bs(kctx, "Error in read completion", bserrno);
        return;
	}
    _kvs_start_open_all_blobs(kctx); 
}

static void
_blob_read_super_page_complete(void* ctx, int bserrno){
    struct kvs_start_ctx *kctx = ctx;
    if (bserrno) {
		_unload_bs(kctx, "Error in read completion", bserrno);
        return;
	}

    if(kctx->sl->kvs_pin != DEFAULT_KVS_PIN){
        _unload_bs(kctx, "Not a valid kvs pin", -EINVAL);
        return;
    }

    if(kctx->sl->nb_shards%kctx->opts->nb_works!=0){
        _unload_bs(kctx, "Works mismatch", -EINVAL);
        return;
    }

    uint32_t super_size = sizeof(struct super_layout) + 
                    kctx->sl->nb_shards * kctx->sl->nb_slabs_per_shard * sizeof(struct slab_layout);
    spdk_free(kctx->sl);
    kctx->sl = spdk_malloc(KV_ALIGN(super_size,0x1000u),0x1000,NULL,SPDK_ENV_LCORE_ID_ANY,SPDK_MALLOC_DMA);
    assert(kctx->sl!=NULL);

    uint32_t nb_pages = KV_ALIGN(super_size,0x1000u)/0x1000u;

    spdk_blob_io_read(kctx->super_blob,kctx->channel,kctx->sl,0,nb_pages,_blob_read_all_super_pages_complete,kctx);
}

static void
_kvs_start_super_open_complete(void*ctx, struct spdk_blob *blob, int bserrno){
    struct kvs_start_ctx *kctx = ctx;
    if (bserrno) {
        _unload_bs(kctx, "Error in open super completion",bserrno);
        return;
    }
    if(spdk_blob_get_num_clusters(blob)==0){
        //Bad super blob
        _unload_bs(kctx, "Empty super blob",bserrno);
        return;
    }
    kctx->super_blob = blob;
    kctx->channel = spdk_get_io_channel(kctx->bs);
    kctx->sl = spdk_malloc(kctx->io_unit_size, 0x1000, NULL,
					SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    assert(kctx->sl!=NULL);

    spdk_blob_io_read(blob,kctx->channel,kctx->sl,0,1,_blob_read_super_page_complete,kctx);
}

static void
_kvs_start_get_super_complete(void *ctx,spdk_blob_id blobid,int bserrno){
    struct kvs_start_ctx *kctx = ctx;

	if (bserrno) {
        char *msg = bserrno == -ENOENT ? "Root blob not found" : "Error in get_super callback";
		_unload_bs(kctx, msg,bserrno);
		return;
	}
	kctx->super_blob_id = blobid;
    spdk_bs_open_blob(kctx->bs,blobid,_kvs_start_super_open_complete,kctx);
}

static void
_kvs_start_load_bs_complete(void *ctx, struct spdk_blob_store *bs, int bserrno){
    if (bserrno) {
        _unload_bs(NULL, "Error in load callback",bserrno);
        return;
	}
    uint64_t io_unit_size = spdk_bs_get_io_unit_size(bs);
    if(io_unit_size!=KVS_PAGE_SIZE){
        SPDK_ERRLOG("Not a valid-formated kvs!!\n");
        SPDK_ERRLOG("Supported unit size: %d. Yours:%" PRIu64 "\n",KVS_PAGE_SIZE,io_unit_size);
        spdk_app_stop(-1);
		return;
    }

    struct kvs_start_ctx* kctx = malloc(sizeof(struct kvs_start_ctx));
    kctx->bs = bs;
    kctx->opts = ctx;

    kctx->io_unit_size = io_unit_size;
    spdk_bs_get_super(bs,_kvs_start_get_super_complete,kctx);
}

static void
_kvs_start(void* ctx){
    struct kvs_start_opts *opts = ctx;
    struct spdk_bdev *bdev = NULL;
	struct spdk_bs_dev *bs_dev = NULL;

	bdev = spdk_bdev_get_by_name(opts->devname);
	if (bdev == NULL) {
		SPDK_ERRLOG("Could not find a bdev with name: %s\n",opts->devname);
		spdk_app_stop(-1);
		return;
	}

    uint64_t block_size = spdk_bdev_get_block_size(bdev);
    if(block_size!=KVS_PAGE_SIZE){
        SPDK_ERRLOG("Device is not supported for kvs!!\n");
        SPDK_ERRLOG("Supported page size of kvs: %d. Yours:%" PRIu64 "\n",KVS_PAGE_SIZE,block_size);
        spdk_app_stop(-1);
		return;
    }

	bs_dev = spdk_bdev_create_bs_dev(bdev, NULL, NULL);
	if (bs_dev == NULL) {
		printf("Could not create blob bdev!!\n");
		spdk_app_stop(-1);
		return;
	}

	spdk_bs_load(bs_dev, NULL, _kvs_start_load_bs_complete, opts);
}

static const char*
_get_cpu_mask(uint32_t nb_works){
    const char* mask = NULL;
    
    switch(nb_works){
        case 1:
            mask =  "0x3";
            break;
        case 2:
            mask = "0x7";
            break;
        case 4:
            mask = "0x1f";
            break;
        case 8:
            mask = "0x1ff";
            break;
        case 16:
            mask = "0x1ffff";
            break;
        case 32:
            mask = "0x1ffffffff";
            break;
        case 64:
            mask = "0x1ffffffffffffffff";
            break;
        case 128:
            mask = "0x1ffffffffffffffffffffffffffffffff";
            break;
        default:
            mask = NULL;
            break;
    }
    return mask;
}

void 
kvs_start_loop(struct kvs_start_opts *opts){

	int rc = 0;
    opts->spdk_opts->reactor_mask = _get_cpu_mask(opts->nb_works);
    assert(opts->spdk_opts->reactor_mask!=NULL);
    assert(opts->startup_fn!=NULL);

    assert(!atomic_load(&g_started));
    atomic_store(&g_started,1);

    rc = spdk_app_start(opts->spdk_opts, _kvs_start, opts);
    if (rc) {
        SPDK_NOTICELOG("KVS starts ERROR!\n");
    } else {
        SPDK_NOTICELOG("KVS starts SUCCESS!\n");
    }

	spdk_app_fini();
	//return rc;
}

bool kvs_is_started(void){
    return atomic_load(&g_started) ? true : false;
}
