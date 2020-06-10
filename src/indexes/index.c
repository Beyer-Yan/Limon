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

const uint8_t* mem_index_first(struct mem_index *mem_index, uint32_t *key_len_out){
    art_tree *t = (art_tree*)mem_index;
    art_leaf *leaf = art_minimum(t);
    
    if(leaf){
        *key_len_out = leaf->key_len;
        return leaf->key;
    }
    return NULL;
}

struct _iter_next_data{
    const unsigned char *key;
    uint32_t len;
    struct index_entry *entry;
};

static int _iter_next_cb(void *data, const unsigned char *key, uint32_t key_len, void *value){
    struct _iter_next_data *iter = data;
    iter->key = key;
    iter->len = key_len;
    iter->entry = value;

    //We scan only one item.
    return 1;
}

const uint8_t* mem_index_next(struct mem_index *mem_index,struct kv_item *base_item,uint32_t *key_len_out){
    art_tree *t = (art_tree*)mem_index;
    
    struct _iter_next_data data = {
        .key = NULL,
        .len = 0,
        .entry = NULL
    };

    art_iter_next(t,base_item->data,base_item->meta.ksize,_iter_next_cb,&data);
    
    if(data.entry){
        *key_len_out =  data.len;
        return data.key;
    }
    return NULL;
}
