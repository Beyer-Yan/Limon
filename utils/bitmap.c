
#include <assert.h>
#include "bitmap.h"
#include "kvutil.h"

unsigned char msbmask[] = {
    0xFF, 0xFE, 0xFC, 0xF8,
    0xF0, 0xE0, 0xC0, 0x80
};
unsigned char lsbmask[] = {
    0x01, 0x03, 0x07, 0x0F,
    0x1F, 0x3F, 0x7F, 0xFF
};

uint32_t bitmap_header_size(uint32_t num_bits){
    return sizeof(struct bitmap) + KV_ALIGN(num_bits,64u)/8;
}

uint32_t bitmap_get_length(struct bitmap *map){
    assert(map);
    return map->length;
}

uint32_t
bitmap_get_bit(struct bitmap *map ,uint32_t id){
    assert(map);
	assert(id < map->length);

    uint32_t a = id/8;
    uint32_t b = id%8;
    uint8_t* data = (uint8_t*)map->data;
 
    return (data[a]>>b)&1; 
}

void
bitmap_set_bit(struct bitmap *map,uint32_t id){
    assert(map);
	assert(id < map->length);

    uint32_t a = id/8;
    uint32_t b = id%8;
    uint8_t* data = (uint8_t*)map->data;

   data[a] |= (1<<b);
}

// [a,b], is a closed interval
void
bitmap_set_bit_range(struct bitmap *map,uint32_t lo, uint32_t hi){
    assert(map);
    assert(hi < map->length);
	assert(lo <= hi);

    uint8_t* data = (uint8_t*)map->data;

    if(lo/8<hi/8){
        data[lo/8] |= msbmask[lo%8];
        uint32_t i = lo/8+1;
        for(;i<hi/8;i++){
            data[i] = 0xFF;
        }
        data[hi/8] |= lsbmask[hi%8];
    }else{
        data[lo/8] |= (msbmask[lo%8]&lsbmask[hi%8]);
    }
}

void
bitmap_clear_bit(struct bitmap *map,uint32_t id){
    assert(map);
	assert(id < map->length);

    uint32_t a = id/8;
    uint32_t b = id%8;
    uint8_t* data = (uint8_t*)map->data;

    data[a] &= ~(1<<b);
}

void
bitmap_clear_bit_range(struct bitmap *map,uint32_t lo, uint32_t hi){
    assert(map);
    assert(hi < map->length);
	assert(lo <= hi);

    uint8_t* data = (uint8_t*)map->data;

    if(lo/8<hi/8){
        data[lo/8] &= ~msbmask[lo%8];
        uint32_t i = lo/8+1;
        for(;i<hi/8;i++){
            data[i] = 0;
        }
        data[hi/8] &= ~lsbmask[hi%8];
    }else{
        data[lo/8] &= ~(msbmask[lo%8]&lsbmask[hi%8]);
    }
}

void 
bitmap_clear_bit_all(struct bitmap *map){
    assert(map);
    uint32_t num_ll = KV_ALIGN(map->length,64u)/64;

    uint32_t i=0;
    for(;i<num_ll;i++){
        ((map->data))[i] = 0;
    }
}

uint32_t
bitmap_get_first_clear_bit(struct bitmap *map){
    assert(map);

    uint32_t num_ll =  KV_ALIGN(map->length,64u)/64;
    uint64_t word = UINT64_MAX;

    uint32_t i = 0;
    for(;i<num_ll;i++){
        if( map->data[i]!=UINT64_MAX ){
            word = map->data[i];
            break;
        }
    }
    if(i==num_ll-1 && (map->length%64)!=0 ){
        //Process The last word the last bit is in.
        uint64_t mask = ~(((uint64_t)1 << (map->length%64))-1);
        word =  map->data[i] | mask;
    }

    uint32_t in_word_idx = (uint32_t)__builtin_ffsll(~word);
    return in_word_idx ? in_word_idx-1 + i*64 : UINT32_MAX;
}
