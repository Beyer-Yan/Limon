#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include "mtable.h"
#include "bitmap.h"
#include "hashmap.h"

#define TABLE_CAPACITY 256
#define TABLE_BLOCK_SIZE (32*1024u*1024u)
#define NB_SLOTS_PER_BLOCK ((TABLE_BLOCK_SIZE)/sizeof(uint64_t))

/*************************************************************************/
/* Definition for deleting window */
struct delete_window{
   uint32_t head;
   uint32_t capacity;
   uint64_t sid[0];
};

static inline struct delete_window* delete_window_create(uint32_t cap){
   uint64_t size = sizeof(struct delete_window) + sizeof(uint64_t)*cap;
   struct delete_window* w = (struct delete_window*)calloc(1,size);
   assert(w);

   w->capacity = cap;
   w->head = 0;
   return w;
}

static inline void delete_window_destroy(struct delete_window* w){
   assert(w);
   free(w);
}

static inline uint64_t delete_window_push(struct delete_window* w,uint64_t sid){
   uint64_t old = w->sid[w->head];
   uint64_t next = (w->head+1)%(w->capacity);
   w->sid[next] = sid;

   return old;
}

static inline int delete_window_check_exist(struct delete_window* w,uint64_t sid){
    assert(sid);

    //Just visit one by one since the capacity is, normally, very small
    for(uint32_t i=0;i<w->capacity;i++){
        if(w->sid[i]==sid){
            return 1;
        }
    }
    return 0;
}

/*************************************************************************/
/* Definition for mapping table */

struct sid_internal{
    uint64_t id:8;
    uint64_t block_id:16;
    uint64_t offset:40;
};

static_assert(sizeof(struct sid_internal)==8,"incorrect size for sid_internal");

struct mtable_block{
    struct bitmap* bitmap;
    uint64_t nb_free_slots;
    uint64_t slots[NB_SLOTS_PER_BLOCK];
};

struct mtable{
    uint64_t id;
    uint64_t nb_frees_slots;
    uint64_t nb_total_slots;
    uint64_t nb_used_blocks;

    //Resolve delete conflicts
    struct delete_window *window;  
    struct mtable_block* blocks[TABLE_CAPACITY];
};

static inline void _checking_sid_legality(struct mtable* mt,uint64_t sid){
    assert(mt);
    assert(sid);
    struct sid_internal* _sid = (struct sid_internal*)(&sid);
    assert(_sid->id==mt->id);
    assert(_sid->block_id<mt->nb_used_blocks);

    uint64_t offset = _sid->offset;
    assert(offset<NB_SLOTS_PER_BLOCK);
    assert(mt->blocks[_sid->block_id]);  
}

static inline struct mtable_block*  _alloc_one_block(void){
    struct mtable_block* block = (struct mtable_block*)calloc(1,sizeof(struct mtable_block));
    assert(block);

    //bitmap size
    uint64_t bitmap_size = bitmap_header_size(NB_SLOTS_PER_BLOCK);
    block->bitmap = (struct bitmap*)calloc(1,bitmap_size);
    assert(block->bitmap);
    block->nb_free_slots = NB_SLOTS_PER_BLOCK;
}

static inline void _release_one_block(struct mtable_block* block){
    assert(block);
    assert(block->bitmap);
    free(block->bitmap);
    free(block);
}

uint32_t mtable_get_worker_from_sid(uint64_t sid){
    assert(sid);
    struct sid_internal* _sid = (struct sid_internal*)(&sid);
    return _sid->id;
}  

struct mtable* mtable_new(uint64_t id, uint32_t window_size){
    struct mtable* table = (struct mtable*)calloc(1,sizeof(struct mtable));
    assert(table);

    //init one table block;
    table->id = id;
    table->blocks[0] = _alloc_one_block();
    table->nb_used_blocks = 1;
    table->nb_total_slots = NB_SLOTS_PER_BLOCK;
    table->nb_frees_slots = NB_SLOTS_PER_BLOCK;
    table->window = delete_window_create(window_size);

    return table;
}

void mtable_destroy(struct mtable* mt){
    assert(mt);
    uint64_t i=0;
    for(;i<mt->nb_used_blocks;i++){
        _release_one_block(mt->blocks[i]);
    }
    delete_window_destroy(mt->window);
    free(mt);
}

struct slot_entry* mtable_get(struct mtable* mt,uint64_t sid){
    _checking_sid_legality(mt,sid);

    struct sid_internal* _sid = (struct sid_internal*)(&sid);
    struct mtable_block* block = mt->blocks[_sid->block_id];

    //check wether the slot is occupied;
    assert(bitmap_get_bit(block->bitmap,_sid->offset));
    struct slot_entry* entry = (struct slot_entry*)(&block->slots[_sid->offset]);

    return entry; 
}

int mtable_check_valid(struct mtable* mt,uint64_t sid){ 
    _checking_sid_legality(mt,sid);

    if(delete_window_check_exist(mt->window,sid)){
        return 0;
    }

    struct sid_internal* _sid = (struct sid_internal*)(&sid);
    struct mtable_block* block = mt->blocks[_sid->block_id];

    return bitmap_get_bit(block->bitmap,_sid->offset);
}

uint64_t mtable_alloc_sid(struct mtable* mt, struct slot_entry new_entry){
    assert(mt);
    struct mtable_block* block = NULL;
    uint64_t block_id = TABLE_CAPACITY;

    if(!mt->nb_frees_slots){
        //Allocate one block;
        assert(mt->nb_used_blocks<TABLE_CAPACITY);
        mt->blocks[mt->nb_used_blocks] = _alloc_one_block();
        assert(mt->blocks[mt->nb_used_blocks]);
        mt->nb_total_slots += NB_SLOTS_PER_BLOCK;
        mt->nb_frees_slots += NB_SLOTS_PER_BLOCK;

        block = mt->blocks[mt->nb_used_blocks];
        block_id = mt->nb_used_blocks;

        mt->nb_used_blocks++;
    }

    if(!block){
        uint64_t i=0;
        uint64_t block_id = TABLE_CAPACITY+1;
        for(;i<mt->nb_used_blocks;i++){
            if(mt->blocks[i]->nb_free_slots){
                block = mt->blocks[i];
                block_id = i;
                break;
            }
        }
    }

    assert(block);
    assert(block_id<(TABLE_CAPACITY));
    struct sid_internal _sid;
    uint64_t offset = bitmap_get_first_clear_bit(block->bitmap);

    assert(offset!=UINT32_MAX);

    _sid.id = mt->id;
    _sid.block_id = block_id;
    _sid.offset = offset;

    block->nb_free_slots--;
    mt->nb_frees_slots--;

    //reinterpreted as the uint64_t type;
    uint64_t sid = *((uint64_t*)(&_sid));
    *(struct slot_entry*)(&block->slots[offset]) = new_entry;

    return sid;
}

void mtable_release(struct mtable* mt,uint64_t sid){
    _checking_sid_legality(mt,sid);

    uint64_t old_sid = delete_window_push(mt->window,sid);
    if(old_sid){
        _checking_sid_legality(mt,old_sid);

        struct sid_internal* _sid = (struct sid_internal*)(&old_sid);
        struct mtable_block* block = mt->blocks[_sid->block_id];
        struct slot_entry* entry = (struct slot_entry*)(&block->slots[_sid->offset]);

        //check wether the slot is occupied;
        assert(!bitmap_get_bit(block->bitmap,_sid->offset) && "Release an empty old slot");

        //Now, release the old slot;
        bitmap_clear_bit(block->bitmap,_sid->offset);
        block->nb_free_slots--;
        mt->nb_frees_slots++;
    }
}