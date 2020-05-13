#include "index.h"
#include "art.h"

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

void* mem_index_add(struct mem_index *mem_index, struct kv_item *item,struct index_entry* entry){
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

void mem_index_first(struct mem_index *mem_index,struct kv_item **item_out){
    art_tree *t = (art_tree*)mem_index;
    unsigned char *key;
    uint32_t len;
    struct index_entry *entry;
    static struct kv_item item;

    art_first(t,&key,&len,&entry);
    if(key==NULL){
        *item_out=NULL;
    }
    else{
        item.meta.ksize = len;
        item.data = key;
        *item_out = &item;
    }
}

void mem_index_next(struct mem_index *mem_index,struct kv_item *base_item,struct kv_item **item_out){
    art_tree *t = (art_tree*)mem_index;
    unsigned char *key;
    uint32_t len;
    struct index_entry *entry;
    static struct kv_item item;

    art_find_next(t,base_item->data,base_item->meta.ksize,&key,&len,&entry);
    if(key==NULL){
        *item_out=NULL;
    }
    else{
        item.meta.ksize = len;
        item.data = key;
        *item_out = &item;
    }
}
