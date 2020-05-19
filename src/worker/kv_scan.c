#include <assert.h>
#include "worker_internal.h"
#include "kverrno.h"
#include "index.h"

/*------------------------------------------------*/
// run filter asychronisely.
// For each item, the filter load the item and apply filter to the item.

// enum states {
//     FILTER_START,
//     FILTER_END
// };

// static void
// filter_complete(void* ctx){
//     struct kv_request_internal *req = ctx;
//     req->pctx.state = FILTER_END;
//     run_state_machine(req);
// }

// static void
// _process_get_load_data_cb(void* ctx, int kverrno){
//     struct kv_request_internal *req = ctx;
//     struct process_ctx *pctx = &req->pctx;
//     struct worker_context *wctx = pctx->wctx;
//     struct index_entry *entry = pctx->res->entries[pctx->cnt];
//     struct chunk_desc *desc   = entry->chunk_desc;

//     if(kverrno){
//         //Error hits when load data from disk
//         //Should I tell the user in such case ??
//         req->cb_fn(req->ctx,NULL,-KV_EIO);
//     }
//     assert(pagechunk_is_cached(desc,entry->slot_idx));

//     //Now we load the data into the page chunk cache. Just read it out happily.
//     struct kv_item *item = pagechunk_get_item(wctx->pmgr,desc,entry->slot_idx);
//     bool filtered = true;
//     //if(req->filter){
//     //    filtered = req->filter(item);
//     //}
//     if(filtered){
//         req->cb_fn(req->ctx, item, -KV_ESUCCESS);
//     }

//     entry->scanning = 0;
//     desc->flag &=~ CHUNK_PIN;
//     filter_complete(req);
// }

// static void 
// _process_get_pagechunk_cb(void* ctx, int kverrno){
//     struct kv_request_internal *req = ctx;
//     struct process_ctx *pctx = &req->pctx;
//     struct worker_context *wctx     = pctx->wctx;
//     struct index_entry *entry = pctx->res->entries[pctx->cnt];
//     // We do not care the kverrno, because it must success.
//     // Now, load the data from disk.
//     assert(!kverrno);

//     pagechunk_load_item_async( wctx->pmgr,
//                                wctx->imgr, 
//                                entry->chunk_desc,
//                                entry->slot_idx,
//                                _process_get_load_data_cb,
//                                req);
// }

// static scan_filter_item_async(struct kv_request_internal *req){
//     struct process_ctx *pctx = &req->pctx;
//     struct worker_context *wctx = pctx->wctx;
//     struct index_entry *entry = pctx->res->entries[pctx->cnt];
//     struct chunk_desc *desc = entry->chunk_desc;

//     assert(desc!=NULL);
//     desc->flag |= CHUNK_PIN;

//     if(entry->writing){
//         /**
//          * @brief Incompatible scan operation on writing item.
//          * In the case that the writing item gets a failed data storing, 
//          * the scanned item for that entry will fail to load the newest data.
//          * User shall ensure that scan operation is issued after a successful
//          * puting operation. 
//          * 
//          * Another case is that The item that is being scanned is being reclaimed.
//          * So I choose to wait the end of PUT operation for the item.
//          */
//         pctx->no_scan=1;
//         TAILQ_INSERT_TAIL(&wctx->resubmit_queue,req,link);
//         //req->cb_fn(req->ctx,NULL,-KV_EOP_RACE);
//         //entry->scanning = 0;
//         //filter_complete(req);
//     }
//     else if(!desc->chunk_mem){
//         //Page chunk is evicted , Now request a new page chunk memory
//         pagechunk_request_one_async(wctx->pmgr,desc,_process_get_pagechunk_cb,req);
//     }
//     else if(pagechunk_is_cached(desc,entry->slot_idx)){
//         //Wonderful! The item is in the page chunk cache.
//         _process_get_load_data_cb(req,-KV_ESUCCESS);
//     }
//     else{
//         //load the data from disk into page chunk
//         pagechunk_load_item_async(wctx->pmgr,wctx->imgr, desc,entry->slot_idx,_process_get_load_data_cb,req);
//     }
// }

// static void scan_finish(struct kv_request_internal *req){
//     struct process_ctx *pctx = &req->pctx;
//     req->cb_fn(req->ctx,NULL,-KV_ESCAN_FINISH);

    
//     int i = 0;
//     for(;i<pctx->res->nb_entries;i++){
//         free(pctx->res->item[i]);
//     }
    
//     free(pctx->res);
//     pool_release(pctx->wctx->kv_request_internal_pool,req);
// }

// static void run_state_machine(struct kv_request_internal *req){
//     struct process_ctx *pctx = &req->pctx;
//     struct iomgr* imgr = pctx->wctx->imgr;
//     uint32_t a_ios = 0;;

//     enum states prev_state;
//     do{
//         prev_state = pctx->state;
//         switch(pctx->state){
//             case FILTER_START:
//                 a_ios = imgr->max_pending_io - imgr->nb_pending_io;
//                 if(a_ios>0){
//                     scan_filter_item_async(req);
//                 }
//                 else{
//                     //No enough io resources. Just wait
//                     pctx->no_scan=1;
//                     TAILQ_INSERT_TAIL(&pctx->wctx->resubmit_queue,req,link);
//                 }
//                 break;
//             case FILTER_END:
//                 if(pctx->cnt++ < pctx->res->nb_entries){
//                     pctx->state = FILTER_START;
//                 }
//                 else{
//                     scan_finish(req);
//                 }
//                 break;
//         }
//     }while(prev_state!=pctx->state);
// }

// void worker_process_scan_filter_async(struct kv_request_internal *req){
//     struct worker_context *wctx = req->pctx.wctx;
//     if(req->pctx.no_scan){
//         run_state_machine(req);
//         return;
//     }
//     struct scan_result *res = mem_index_scan(wctx->mem_index,req->item,req->scan_size);
//     req->pctx.res = res;
//     req->pctx.state = FILTER_START;
//     req->pctx.cnt = 0;

//     run_state_machine(req);
// }

// // run filter sychronisely.
// // For each item, the in-storage-engine filter is not performed, and the item is not loaded.
// // The user has to perform Get for each item.
// // This is the prefered scan mode.

// void worker_process_scan(struct kv_request_internal *req){
//     struct worker_context *wctx = req->pctx.wctx;
//     struct scan_result *res = mem_index_scan(wctx->mem_index,req->item,req->scan_size);
//     int i = 0;
//     for(;i<res->nb_entries;i++){
//         req->cb_fn(req->ctx,res->item[i],-KV_ESUCCESS);
//         res->entries[i]->scanning = 0;
//     }

//     req->cb_fn(req->ctx,NULL,-KV_ESCAN_FINISH);
    
//     for(i=0;i<res->nb_entries;i++){
//         free(res->item[i]);
//     }
    
//     free(res);
//     pool_release(wctx->kv_request_internal_pool,req);
// }

/**
 * @brief It is a global item data buffer, the caller shall copy it
 * to other place when nessacery.
 * The buffer will be flushed whenever the next scan request is performed.
 */
static uint64_t _scan_data[MAX_SLAB_SIZE];

void worker_process_first(struct kv_request_internal *req){
    struct worker_context *wctx = req->pctx.wctx;
    struct kv_item *item = (struct kv_item*)_scan_data;
    uint8_t* key;
    uint32_t key_len;

    key = mem_index_first(wctx->mem_index,&key_len);
    if(key){
        item->meta.ksize = key_len;
        memcpy(item->data,key,key_len);
    }

    req->cb_fn(req->ctx,item,key!=NULL?-KV_ESUCCESS:KV_EMPTY);

    pool_release(wctx->kv_request_internal_pool,req);
}

void worker_process_seek(struct kv_request_internal *req){
    struct worker_context *wctx = req->pctx.wctx;
    struct index_entry *entry;

    entry = mem_index_lookup(wctx->mem_index,req->item);
    req->cb_fn(req->ctx,NULL,entry!=NULL?-KV_ESUCCESS:KV_EITEM_NOT_EXIST);

    pool_release(wctx->kv_request_internal_pool,req);
}

void worker_process_next(struct kv_request_internal *req){
    struct worker_context *wctx = req->pctx.wctx;
    struct kv_item *item = (struct kv_item*)_scan_data;
    uint8_t* key;
    uint32_t key_len;

    key = mem_index_next(wctx->mem_index,req->item,&key_len);
    if(key){
        item->meta.ksize = key_len;
        memcpy(item->data,key,key_len);
    }

    req->cb_fn(req->ctx,item,key!=NULL?-KV_ESUCCESS:KV_EMPTY);

    pool_release(wctx->kv_request_internal_pool,req);
}
