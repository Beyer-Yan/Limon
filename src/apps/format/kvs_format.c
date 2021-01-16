#include <stdint.h>
#include <stdbool.h>
#include "slab.h"
#include "kvutil.h"

#include "spdk/stdinc.h"

#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/blob_bdev.h"
#include "spdk/blob.h"
#include "spdk/log.h"
#include "spdk/string.h"

static char *_g_kvs_name = "kvs_v1.0";

static const char *_g_kvs_getopt_string = "K:S:C:fD:N:E"; 

static struct option _g_app_long_cmdline_options[] = {
#define MAX_KEY_LENGH_OPT_IDX   'K'
    {"max-key-length",optional_argument,NULL,MAX_KEY_LENGH_OPT_IDX},
#define SHARDS_OPT_IDX          'S'
    {"shards",optional_argument,NULL,SHARDS_OPT_IDX},
#define CHUNKS_PER_NODE_OPD_IDX 'C'
    {"chunks-per-node",optional_argument,NULL,CHUNKS_PER_NODE_OPD_IDX},
#define FORCE_FORMAT_OPT_IDX    'f'
    {"force-format",optional_argument,NULL,FORCE_FORMAT_OPT_IDX},
#define DEVNAME_OPT_IDX         'D'
    {"devname",required_argument,NULL,DEVNAME_OPT_IDX},
#define INIT_NODES_IDX          'N'
    {"init-nodes",optional_argument,NULL,INIT_NODES_IDX},
#define DUMP_OPT_IDX            'E'
    {"dump",optional_argument,NULL,DUMP_OPT_IDX}
};

struct kvs_create_opts{
    //should be 1,2,4,8,16,32,64...
    char* devname;
    uint32_t nb_shards;
    uint32_t max_key_length;
    uint32_t nb_chunks_per_reclaim_node;
    uint32_t nb_init_nodes_per_slab;
    bool force_format;
    bool dump_only;
};

static struct kvs_create_opts _g_default_opts = {
    .nb_shards = 64,
    .max_key_length = 256,
    .nb_chunks_per_reclaim_node = 4,
    .nb_init_nodes_per_slab = 10,
    .force_format = false,
    .dump_only = false,
    .devname = "bdev_pmem0"
};

struct kvs_format_ctx{
    struct spdk_blob_store *bs;
    spdk_blob_id super_blob_id;
	struct spdk_blob *super_blob;
	uint64_t io_unit_size;
    uint64_t bs_page_size;
    uint64_t io_unit_per_page;
    uint32_t nb_init_nodes_per_slab;
	int rc;

    struct spdk_io_channel *channel;

    char* devname;
    uint32_t super_size;
    struct super_layout *sl;

    uint32_t *slab_size_array;
    uint32_t nb_slabs;
};

struct _blob_iter{
    struct kvs_format_ctx *kctx;
    int op_type;
    uint32_t slab_idx;
    uint32_t total_slabs;
    void(*finished_cb)(struct kvs_format_ctx* kctx);
    void* ctx;
    struct super_layout *sl;
};

#define KVS_OP_CREATE   0
#define KVS_OP_OPEN     1
#define KVS_OP_RESIZE   2
#define KVS_OP_ZERO     3
#define KVS_OP_CLOSE    4


static void
_unload_complete(void *ctx, int bserrno){
	if (bserrno) {
		SPDK_ERRLOG("Error %d unloading the bobstore\n", bserrno);
	}

	spdk_app_stop(bserrno);
}

static void
_unload_bs(struct kvs_format_ctx *kctx, char *msg, int bserrno)
{
	if (bserrno) {
		SPDK_ERRLOG("%s (err %d)\n", msg, bserrno);
	}
    if(!kctx){
        spdk_app_stop(bserrno);
        return;
    }
	if (kctx->bs) {
		if (kctx->channel) {
			spdk_bs_free_io_channel(kctx->channel);
		}
		spdk_bs_unload(kctx->bs, _unload_complete, NULL);
	} else {
		spdk_app_stop(bserrno);
	}
    if(kctx->sl){
        spdk_free(kctx->sl);
    }
    free(kctx);
}

static void _slab_iter_foreach_next(struct _blob_iter *iter);

static void
_slab_iter_foreach_create_cb(void* ctx, spdk_blob_id blobid,int bserrno){
    struct _blob_iter *iter = ctx;
    struct kvs_format_ctx *kctx = iter->kctx;

    if (bserrno) {
        free(ctx);
        _unload_bs(kctx, "Error in blob create callback", bserrno);
        return;
    }

    struct slab_layout* slab_base = &iter->sl->slab[iter->slab_idx];
    uint32_t nb_slabs_per_shard = iter->sl->nb_slabs_per_shard;

    slab_base->slab_size = iter->kctx->slab_size_array[iter->slab_idx%nb_slabs_per_shard];
    slab_base->blob_id = blobid;
    SPDK_NOTICELOG("new blob id %" PRIu64 " for shard:%u,slab:%u,\n", 
                           blobid,iter->slab_idx/nb_slabs_per_shard,iter->slab_idx%nb_slabs_per_shard);
    
    _slab_iter_foreach_next(iter);
}

static void
_slab_iter_foreach_open_cb(void*ctx, struct spdk_blob* blob, int bserrno){
    struct _blob_iter *iter = ctx;
    struct kvs_format_ctx *kctx = iter->kctx;

    if (bserrno) {
        free(iter);
        _unload_bs(kctx, "Error in blob open callback", bserrno);
        return;
    }

    struct slab_layout *slab_base = &iter->sl->slab[iter->slab_idx];
    slab_base->resv = (uint64_t)blob;
    
    _slab_iter_foreach_next(iter);
}

static void
_slab_iter_foreach_resize_cb(void* ctx,int bserrno){
    struct _blob_iter *iter = ctx;
    struct kvs_format_ctx *kctx = iter->kctx;

    if (bserrno) {
        free(iter);
        _unload_bs(kctx, "Error in blob resize callback", bserrno);
        return;
    }

    _slab_iter_foreach_next(iter);
}

static void
_slab_iter_foreach_zero_cb(void* ctx,int bserrno){
    struct _blob_iter *iter = ctx;
    struct kvs_format_ctx *kctx = iter->kctx;

    if (bserrno) {
        free(iter);
        _unload_bs(kctx, "Error in blob resize callback", bserrno);
        return;
    }

    _slab_iter_foreach_next(iter);
}

static void
_slab_iter_foreach_close_cb(void* ctx,int bserrno){
    struct _blob_iter *iter = ctx;
    struct kvs_format_ctx *kctx = iter->kctx;

    if (bserrno) {
        free(iter);
        _unload_bs(kctx, "Error in blob close callback", bserrno);
        return;
    }

    _slab_iter_foreach_next(iter);
}

static void
_slab_iter_do_op(struct slab_layout* slab,int op_type, struct _blob_iter *iter){
    switch(op_type){
        case KVS_OP_CREATE:{
            spdk_bs_create_blob(iter->kctx->bs,_slab_iter_foreach_create_cb,iter);
            break;
        }
        case KVS_OP_OPEN:{
            spdk_bs_open_blob(iter->kctx->bs,slab->blob_id,_slab_iter_foreach_open_cb,iter);
            break;
        }
        case KVS_OP_RESIZE:{
            uint32_t chunks = iter->kctx->nb_init_nodes_per_slab*iter->kctx->sl->nb_chunks_per_reclaim_node;
            spdk_blob_resize(slab->resv,chunks,_slab_iter_foreach_resize_cb,iter);
            break;
        }
        case KVS_OP_ZERO:{
            uint32_t chunks = iter->kctx->nb_init_nodes_per_slab*iter->kctx->sl->nb_chunks_per_reclaim_node;
            uint32_t nb_pages = chunks * iter->kctx->sl->nb_pages_per_chunk;
            uint64_t nb_blocks = nb_pages*iter->kctx->io_unit_per_page;
            spdk_blob_io_write_zeroes(slab->resv,iter->kctx->channel,0,nb_blocks,_slab_iter_foreach_zero_cb,iter);
            break;
        }
        case KVS_OP_CLOSE:{
            spdk_blob_close(slab->resv,_slab_iter_foreach_close_cb,iter);
            break;
        }
        default:{
            SPDK_ERRLOG("Wrong op type\n");
            assert(0);
            break;
        }
    }
}

static void
_slab_iter_foreach_next(struct _blob_iter *iter){
    struct slab_layout* slab_base = &iter->sl->slab[iter->slab_idx];
    struct kvs_format_ctx *kctx = iter->kctx;
    
    if(iter->slab_idx==iter->total_slabs-1){
        //All slab have been itered;
        void(*finished_cb)(struct kvs_format_ctx *kctx) = iter->finished_cb;
        free(iter);
        finished_cb(kctx);
    }
    else{
        //Iter the next slab.
        iter->slab_idx++;
        slab_base = &iter->sl->slab[iter->slab_idx];
        _slab_iter_do_op(slab_base,iter->op_type,iter);
    }
}

static void
_slab_iter_foreach(struct kvs_format_ctx *kctx,int op_type,void(*finished_cb)(struct kvs_format_ctx *kctx)){
    struct _blob_iter *iter = malloc(sizeof(struct _blob_iter));
    assert(iter!=NULL);

    iter->kctx = kctx;
    iter->slab_idx = 0;
    iter->finished_cb = finished_cb;
    iter->op_type = op_type;
    iter->total_slabs = kctx->sl->nb_shards * kctx->sl->nb_slabs_per_shard;
    iter->sl = kctx->sl;

    struct slab_layout *slab_base =  &iter->sl->slab[0];
    _slab_iter_do_op(slab_base,op_type,iter);
}

static void
_kvs_dump_slab_all_close_complete(struct kvs_format_ctx *kctx){
    _unload_bs(kctx,"",0);
}

static void
_kvs_close_super_complete(void*ctx, int bserrno){
    struct kvs_format_ctx *kctx = ctx;
    if (bserrno) {
        _unload_bs(kctx, "Error in blob close callback", bserrno);
        return;
    }

    _slab_iter_foreach(kctx,KVS_OP_CLOSE,_kvs_dump_slab_all_close_complete);
}

static void
_kvs_dump_one_slab(struct slab_layout* slab){
    struct spdk_blob *blob = (struct spdk_blob*)(slab->resv);
    printf("\t-------------------\n");
    printf("\tblob id:%" PRIu64 "\n",slab->blob_id);
    printf("\tblob addr:%" PRIx64 "\n",slab->resv);
    printf("\tslab size:%u\n",slab->slab_size);

    uint64_t total_chunks = spdk_blob_get_num_clusters(blob);
    printf("\ttotal chunks:%" PRIu64 "\n",total_chunks);
}

static void
_kvs_dump_real_data(struct kvs_format_ctx *kctx){

    printf("kvs global configuration:\n");
    printf("\tkvs pin code:%" PRIx64 "\n",kctx->sl->kvs_pin);
	printf("\tshards count: %u\n", kctx->sl->nb_shards);
    printf("\tslabs per shard:%u\n",kctx->sl->nb_slabs_per_shard);
    printf("\tchunks per reclaim node:%u\n",kctx->sl->nb_chunks_per_reclaim_node);
    printf("\tpages per chunk:%u\n",kctx->sl->nb_pages_per_chunk);
    printf("\tpage size:%" PRIu64 "\n",kctx->io_unit_size);
    printf("\tmax key length:%u\n",kctx->sl->max_key_length);

    uint64_t total_chunks = spdk_bs_total_data_cluster_count(kctx->bs);
    uint64_t free_chunks = spdk_bs_free_cluster_count(kctx->bs);
    printf("\ttotal chunks:%" PRIu64 "\n",total_chunks);
    printf("\tfree chunks:%" PRIu64 "\n\n",free_chunks);

    printf("slab info:\n");
    uint32_t i=0,j=0;
    uint32_t shards = kctx->sl->nb_slabs_per_shard;
    for(;i<kctx->sl->nb_shards;i++){
        struct slab_layout* shard_base = (struct slab_layout*)(kctx->sl + 1) + shards * i;
        printf("shard:%u ------\n",i);
        for(j=0;j<kctx->sl->nb_slabs_per_shard;j++){
            struct slab_layout *slab = shard_base + j;
            _kvs_dump_one_slab(slab);
        }
    }
    spdk_blob_close(kctx->super_blob,_kvs_close_super_complete,kctx);;
}

static void
_blob_dump_super_complete(void* ctx, int bserrno){
    struct kvs_format_ctx *kctx = ctx;
    if (bserrno) {
		_unload_bs(kctx, "Error in read completion", bserrno);
        return;
	}
    _slab_iter_foreach(kctx,KVS_OP_OPEN,_kvs_dump_real_data);
}

static void
_blob_dump_read_super_page_complete(void* ctx, int bserrno){
    struct kvs_format_ctx *kctx = ctx;
    if (bserrno) {
		_unload_bs(kctx, "Error in read completion", bserrno);
        return;
	}

    if(kctx->sl->kvs_pin != DEFAULT_KVS_PIN){
        _unload_bs(kctx, "Not a valid kvs pin", bserrno);
        return;
    }

    uint32_t super_size = sizeof(struct super_layout) + 
                    kctx->sl->nb_shards * kctx->sl->nb_slabs_per_shard * sizeof(struct slab_layout);
    spdk_free(kctx->sl);
    kctx->sl = spdk_malloc(KV_ALIGN(super_size,0x1000u),0x1000,NULL,SPDK_ENV_LCORE_ID_ANY,SPDK_MALLOC_DMA);
    assert(kctx->sl!=NULL);

    uint32_t nb_pages = KV_ALIGN(super_size,0x1000u)/0x1000u;
    uint64_t nb_blocks = nb_pages*kctx->io_unit_per_page;

    spdk_blob_io_read(kctx->super_blob,kctx->channel,kctx->sl,0,nb_blocks,_blob_dump_super_complete,kctx);
}

static void
_kvs_dump_super_complete(void*ctx, struct spdk_blob *blob, int bserrno){
    struct kvs_format_ctx *kctx = ctx;
    if (bserrno) {
        _unload_bs(kctx, "Error in open super completion",bserrno);
        return;
    }
    kctx->super_blob = blob;
    kctx->channel = spdk_get_io_channel(kctx->bs);
    kctx->sl = spdk_malloc(kctx->bs_page_size, 0x1000, NULL,
					SPDK_ENV_LCORE_ID_ANY, SPDK_MALLOC_DMA);
    assert(kctx->sl!=NULL);

    //read one page from the start
    uint64_t nb_blocks = 1*kctx->io_unit_per_page;

    spdk_blob_io_read(blob,kctx->channel,kctx->sl,0,nb_blocks,_blob_dump_read_super_page_complete,kctx);
}

static void
_kvs_dump_get_super_complete(void *ctx,spdk_blob_id blobid,int bserrno){
    struct kvs_format_ctx *kctx = ctx;

	if (bserrno) {
        char *msg = bserrno == -ENOENT ? "Root blob not found" : "Error in get_super callback";
		_unload_bs(kctx, msg,bserrno);
		return;
	}
	kctx->super_blob_id = blobid;
    spdk_bs_open_blob(kctx->bs,blobid,_kvs_dump_super_complete,kctx);
}

static void
_kvs_dump_load_complete(void *ctx, struct spdk_blob_store *bs, int bserrno){
    if (bserrno) {
        _unload_bs(NULL, "Error in load callback",bserrno);
        return;
	}
    struct kvs_format_ctx* kctx = malloc(sizeof(struct kvs_format_ctx));
    kctx->bs = bs;
    kctx->devname = ctx;
    kctx->sl = NULL;
    kctx->io_unit_size = spdk_bs_get_io_unit_size(bs);
    kctx->bs_page_size = spdk_bs_get_page_size(bs);

    assert(kctx->bs_page_size%kctx->io_unit_size==0);
    assert(kctx->bs_page_size==KVS_PAGE_SIZE);

    kctx->io_unit_per_page = kctx->bs_page_size/kctx->io_unit_size;
    
    spdk_bs_get_super(bs,_kvs_dump_get_super_complete,kctx);
}

static void
_kvs_dump(void*ctx){
    const char* devname = (const char*)ctx;
    struct spdk_bdev *bdev = NULL;
	struct spdk_bs_dev *bs_dev = NULL;

	bdev = spdk_bdev_get_by_name(devname);
	if (bdev == NULL) {
		printf("Could not find a bdev\n");
		spdk_app_stop(-1);
		return;
	}

	bs_dev = spdk_bdev_create_bs_dev(bdev, NULL, NULL);
	if (bs_dev == NULL) {
		printf("Could not create blob bdev!!\n");
		spdk_app_stop(-1);
		return;
	}

	spdk_bs_load(bs_dev, NULL, _kvs_dump_load_complete, devname);
}

/***********************************************************************/

static void
_slab_all_close_complete(struct kvs_format_ctx *kctx){
     _unload_bs(kctx, "",0);
}

static void
_super_blob_close_complete(void*ctx, int bserrno){
    struct kvs_format_ctx *kctx = ctx;
    if (bserrno) {
		_unload_bs(kctx, "Error in closing super blob",bserrno);
		return;
	}

    if(kctx->nb_init_nodes_per_slab>0){
        //I should close all slab blobs.
        _slab_iter_foreach(kctx,KVS_OP_CLOSE,_slab_all_close_complete);
    }
}

static void
_super_sync_complete(void*ctx, int bserrno){
    struct kvs_format_ctx *kctx = ctx;
    
    if (bserrno) {
		_unload_bs(kctx, "Error in sync callback",bserrno);
		return;
	}
    spdk_blob_close(kctx->super_blob,_super_blob_close_complete,kctx);
}

static void
_super_write_complete(void *ctx, int bserrno){
    struct kvs_format_ctx *kctx = ctx;
    
    if (bserrno) {
        _unload_bs(kctx, "Error in write completion", bserrno);
        return;
	}

    spdk_blob_sync_md(kctx->super_blob, _super_sync_complete, kctx);
}

static void
_do_super_write(struct kvs_format_ctx *kctx){
    uint32_t nb_pages = KV_ALIGN(kctx->super_size,0x1000u)/0x1000u;
    uint64_t nb_blocks = nb_pages*kctx->io_unit_per_page;
    spdk_blob_io_write(kctx->super_blob,kctx->channel,kctx->sl,0,nb_blocks,_super_write_complete,kctx);
}

static void
_slab_all_resize_complete(struct kvs_format_ctx *kctx){
    _slab_iter_foreach(kctx,KVS_OP_ZERO,_do_super_write);
}

static void
_slab_all_open_complete(struct kvs_format_ctx *kctx){
    //Resize all slabs with init nodes.
    //When all slabs have been resized, just write the super blob.
    _slab_iter_foreach(kctx,KVS_OP_RESIZE,_slab_all_resize_complete);
}

static void
_slab_all_create_complete(struct kvs_format_ctx *kctx){
    if(kctx->nb_init_nodes_per_slab>0){
        //When perform slab resizing, I shall open them all firstly.
        _slab_iter_foreach(kctx,KVS_OP_OPEN,_slab_all_open_complete);
    }
    else{
        //I needn't perform init slab resizing.
        _do_super_write(kctx);
    }
}

static void
_super_resize_complete(void *ctx, int bserrno){
    struct kvs_format_ctx *kctx = ctx;
    if (bserrno) {
		_unload_bs(kctx, "Error in super blob resize", bserrno);
		return;
	}
    //Create one blob for each slab.
    //When all slabs have been created, echo slab should be resized equal to 
    //the init size;
    _slab_iter_foreach(kctx,KVS_OP_CREATE,_slab_all_create_complete);
}

static void
_super_blob_open_complete(void *ctx, struct spdk_blob *blob, int bserrno){
	struct kvs_format_ctx *kctx = ctx;

	if (bserrno) {
		_unload_bs(kctx, "Error in open super blob",bserrno);
		return;
	}

	kctx->super_blob = blob;
    
    kctx->channel = spdk_bs_alloc_io_channel(kctx->bs);
	if (kctx->channel == NULL) {
		_unload_bs(kctx, "Error in allocating channel",-ENOMEM);
		return;
	}

    uint32_t super_size = kctx->super_size;
    uint32_t nb_pages = KV_ALIGN(super_size,0x1000u)/KVS_PAGE_SIZE;
    uint32_t chunk_pages = kctx->sl->nb_pages_per_chunk;
    uint32_t nb_clusters = nb_pages/chunk_pages + (!!(nb_pages%chunk_pages));

    spdk_blob_resize(kctx->super_blob, nb_clusters, _super_resize_complete, kctx);
}

static void
_set_super_root_complete(void*ctx, int bserrno){
    struct kvs_format_ctx *kctx = ctx;
    if (bserrno) {
        _unload_bs(kctx, "Error in set_super callback",bserrno);
        return;
	}
	/* We have to open the blob before we can do things like resize. */
	spdk_bs_open_blob(kctx->bs, kctx->super_blob_id,
			  _super_blob_open_complete, kctx);
}

static void 
_super_blob_create_complete(void* ctx, spdk_blob_id blobid,int bserrno){
	struct kvs_format_ctx *kctx = ctx;

	if (bserrno) {
		_unload_bs(kctx, "Error in blob create callback",bserrno);
		return;
	}

	kctx->super_blob_id = blobid;
	SPDK_NOTICELOG("new blob id %" PRIu64 "\n", kctx->super_blob_id);

    spdk_bs_set_super(kctx->bs,blobid,_set_super_root_complete,kctx);
}

static void
_create_super_blob(struct kvs_format_ctx *kctx){
    spdk_bs_create_blob(kctx->bs,_super_blob_create_complete,kctx);
}


static void
bs_init_complete(void *ctx, struct spdk_blob_store *bs, int bserrno){

	struct kvs_format_ctx *kctx = ctx;

	if (bserrno) {
		_unload_bs(kctx, "Error init'ing the blobstore",bserrno);
		return;
	}

	kctx->bs = bs;
	SPDK_NOTICELOG("blobstore: %p\n", kctx->bs);

    uint64_t io_unit_size = spdk_bs_get_io_unit_size(kctx->bs);
    if(io_unit_size!=KVS_PAGE_SIZE){
        SPDK_WARNLOG("IO unit size is not 4KB!! Yours:%" PRIu64 "\n",io_unit_size);
        //spdk_app_stop(-1);
		//return;
    }
    kctx->io_unit_size = io_unit_size;

    uint64_t bs_page_size = spdk_bs_get_page_size(kctx->bs);
    if(bs_page_size!=KVS_PAGE_SIZE){
        SPDK_ERRLOG("Blobstore page size is not 4KB!! Yours:%" PRIu64 "\n",io_unit_size);
        spdk_app_stop(-1);
        return;
    }
    kctx->bs_page_size = bs_page_size;
    kctx->io_unit_per_page = bs_page_size/io_unit_size;

    assert(kctx->bs_page_size%kctx->io_unit_size==0);

	_create_super_blob(kctx);
}

static void
_fill_super_parameters(struct kvs_format_ctx *kctx){

    uint32_t nb_slabs;
    uint32_t *slab_size_array;
    uint32_t chunk_pages;
    struct kvs_create_opts *kc_opts = &_g_default_opts;

    slab_get_slab_conf(&slab_size_array,&nb_slabs, &chunk_pages);

    uint32_t super_size = sizeof(struct super_layout) + 
                    kc_opts->nb_shards * nb_slabs * sizeof(struct slab_layout);

    struct super_layout *sl = spdk_malloc(KV_ALIGN(super_size,0x1000u),0x1000,NULL,
                            SPDK_ENV_LCORE_ID_ANY,SPDK_MALLOC_DMA);
    assert(sl!=NULL);

    sl->kvs_pin = DEFAULT_KVS_PIN;
    sl->nb_shards = kc_opts->nb_shards;
    sl->nb_slabs_per_shard =  nb_slabs;
    sl->nb_chunks_per_reclaim_node = kc_opts->nb_chunks_per_reclaim_node;
    sl->nb_pages_per_chunk = chunk_pages;
    sl->max_key_length = kc_opts->max_key_length;  
    
    kctx->sl = sl;
    kctx->super_size = super_size;
    kctx->devname = kc_opts->devname;
    kctx->nb_init_nodes_per_slab = kc_opts->nb_init_nodes_per_slab;

    kctx->slab_size_array = slab_size_array;
    kctx->nb_slabs = nb_slabs;
}

static void 
_kvs_create(void*ctx){
    struct kvs_format_ctx *kctx = ctx;

	struct spdk_bdev *bdev = NULL;
	struct spdk_bs_dev *bs_dev = NULL;
    struct spdk_bs_opts bs_opts;

    _fill_super_parameters(kctx);

	bdev = spdk_bdev_get_by_name(kctx->devname);
	if (bdev == NULL) {
		SPDK_ERRLOG("Could not find a bdev:%s\n",kctx->devname);
		spdk_app_stop(-1);
		return;
	}

    uint32_t block_size = spdk_bdev_get_block_size(bdev);
    if(block_size!=KVS_PAGE_SIZE){
        SPDK_WARNLOG("Block size is not 4KB!! Yours:%u\n",block_size);
        //spdk_app_stop(-1);
		//return;
    }

	bs_dev = spdk_bdev_create_bs_dev(bdev, NULL, NULL);
	if (bs_dev == NULL) {
		SPDK_ERRLOG("Could not create blob bdev!!\n");
		spdk_app_stop(-1);
		return;
	}

    spdk_bs_opts_init(&bs_opts,sizeof(bs_opts));
    memcpy(bs_opts.bstype.bstype,_g_kvs_name,strlen(_g_kvs_name));
    bs_opts.cluster_sz = kctx->sl->nb_pages_per_chunk * KVS_PAGE_SIZE;

	spdk_bs_init(bs_dev, &bs_opts, bs_init_complete, kctx);
}

static int
_kvs_parse_arg(int ch, char *arg){
    switch(ch){
        case 'K':{
            long key_length = spdk_strtol(arg,0);
            if(key_length>0){
                _g_default_opts.max_key_length = key_length;
            }
            else{
                fprintf(stderr,"The max-key-length shall be a positive number\n");
                return -EINVAL;
            }
            break;
        }
        case 'S':{
            long shards  = spdk_strtol(arg,0);
            if( (shards>0) && ( (shards&(shards-1))==0 ) ){
                _g_default_opts.nb_shards = shards;
            }
            else{
                fprintf(stderr,"The shards shall be a positive number and be 2 to the power of n \n");
                return -EINVAL;
            }
            break;
        }

        case 'C':{
            long chunks = spdk_strtol(arg,0);
            if(chunks>0){
                _g_default_opts.nb_chunks_per_reclaim_node = chunks;
            }
            else{
                fprintf(stderr,"The shards shall be a positive number\n");
                return -EINVAL;
            }
            break;
        }
        case 'f':
            _g_default_opts.force_format = true;
            break;
        case 'D':
            _g_default_opts.devname = arg;
            break;
        case 'E':
            _g_default_opts.dump_only = true;
            break;
        case 'N':{
            long nodes = spdk_strtol(arg,0);
            if(nodes>0){
                _g_default_opts.nb_init_nodes_per_slab=nodes;
            }
            else{
                fprintf(stderr,"The init nodes shall be a positive number\n");
                return -EINVAL;
            }
            break;
        }
        default:
            return -EINVAL;
            break;
    }
    return 0;
}

static void
_kvs_usage(void){
	printf(" -K, --max-key-length <num>   the max key length of the current kvs(default:%u)\n",
                                          _g_default_opts.max_key_length);
	printf(" -S, --shards <num>           the number of shards(default:%u)\n",
                                          _g_default_opts.nb_shards);
    printf(" -C, --chunks-per-node <num>  the chunks per reclaim node(default:%u)\n",
                                          _g_default_opts.nb_chunks_per_reclaim_node);
    printf(" -f, --force-format           format the kvs forcely\n");
    printf(" -D, --devname <namestr>      the devname(default:%s)\n",
                                          _g_default_opts.devname);
    printf(" -N, --init-nodes  <num>      the init nodes for each slab(default:%u)\n",
                                          _g_default_opts.nb_init_nodes_per_slab);
    printf(" -E, --dump                   Dump the existing kvs format\n");
}

int
main(int argc, char **argv){
    struct spdk_app_opts opts = {0};
	int rc = 0;
	struct kvs_format_ctx *kctx = NULL;

	spdk_app_opts_init(&opts, sizeof(opts));

	opts.name = "kvs_format";
	if ((rc = spdk_app_parse_args(argc, argv, &opts, _g_kvs_getopt_string, _g_app_long_cmdline_options,
				      _kvs_parse_arg, _kvs_usage)) !=
	    SPDK_APP_PARSE_ARGS_SUCCESS) {
		exit(rc);
	}

    if(!_g_default_opts.dump_only){
        kctx = calloc(1, sizeof(struct kvs_format_ctx));
        assert(kctx!=NULL);

        rc = spdk_app_start(&opts, _kvs_create, kctx);
        if (rc) {
            SPDK_NOTICELOG("KVS FORMAT ERROR!\n");
        } else {
            SPDK_NOTICELOG("KVS FORMAT SUCCESS!\n");
        }
    }
    else{
        rc = spdk_app_start(&opts, _kvs_dump, _g_default_opts.devname);
        if (rc) {
            SPDK_NOTICELOG("KVS DUMP ERROR!\n");
        } else {
            SPDK_NOTICELOG("KVS DUMP SUCCESS!\n");
        }
    }
	/* Gracefully close out all of the SPDK subsystems. */
	spdk_app_fini();
	return rc;
}
