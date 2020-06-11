#include <stdlib.h>
#include "index.h"
#include "art.h"
#include "kvutil.h"

struct mem_index{
    art_tree t;
};

struct mem_index* mem_index_init(void){
    struct mem_index* mem_index = malloc(sizeof(struct mem_index));
    if(!mem_index){
        return mem_index;
    }
    art_tree *t = (art_tree*)mem_index;
    art_tree_init(t);
    return mem_index;
}

void mem_index_destroy(struct mem_index* mem_index){
     art_tree *t = (art_tree*)mem_index;
     art_tree_destroy(t);
}

void* mem_index_add(struct mem_index *mem_index, struct kv_item *item, struct index_entry* entry){
    art_tree *t = (art_tree*)mem_index;
    return art_insert(t,item->data,item->meta.ksize,entry);
}

void mem_index_delete(struct mem_index *mem_index,struct kv_item *item){
    art_tree *t = (art_tree*)mem_index;
    art_delete(t,item->data,item->meta.ksize);
}

struct index_entry* mem_index_lookup(struct mem_index *mem_index, struct kv_item *item){
    art_tree *t = (art_tree*)mem_index;
    return art_search(t,item->data,item->meta.ksize);
}

int mem_index_iter(struct mem_index *mem_index,struct kv_item *base_item,mem_cb cb_fn, void*ctx){
    art_tree *t = (art_tree*)mem_index;
    if(!base_item){
        return art_iter(t,cb_fn,ctx);
    }
    else{
        return  art_iter_next(t,base_item->data,base_item->meta.ksize,cb_fn,ctx);
    }
}
