
#include "bitmap.h"
#include <assert.h>

unsigned char msbmask[] = {
    0xFF, 0xFE, 0xFC, 0xF8,
    0xF0, 0xE0, 0xC0, 0x80
};
unsigned char lsbmask[] = {
    0x01, 0x03, 0x07, 0x0F,
    0x1F, 0x3F, 0x7F, 0xFF
};

unsigned char firstbit[] = {
    0,1,0,2,0,1,0,3,
    0,1,0,2,0,1,0
};

uint32_t
bitmap_get_bit(struct bitmap *map ,uint32_t id){
    assert(map);
	assert(id < map->length);

    uint32_t a = id/8;
    uint32_t b = id%8;
 
    return (map->data[a]>>b)&1; 
}

void
bitmap_set_bit(struct bitmap *map,uint32_t id){
    assert(map);
	assert(id < map->length);

    uint32_t a = id/8;
    uint32_t b = id%8;
    map->data[a] |= (1<<b);
}

// [a,b], is a closed interval
void
bitmap_set_bit_range(struct bitmap *map,uint32_t lo, uint32_t hi){
    assert(map);
    assert(hi < map->length);
	assert(lo <= hi);

    if(lo/8<hi/8){
        map->data[lo/8] |= msbmask[lo%8];
        uint32_t i = lo/8+1;
        for(;i<hi/8;i++){
            map->data[i] = 0xFF;
        }
        map->data[hi/8] |= lsbmask[hi%8];
    }else{
        map->data[lo/8] |= (msbmask[lo%8]&lsbmask[hi%8]);
    }
}

void
bitmap_clear_bit(struct bitmap *map,uint32_t id){
    assert(map);
	assert(id < map->length);

    uint32_t a = id/8;
    uint32_t b = id%8;
    map->data[a] &= ~(1<<b);
}

void
bitmap_clear_bit_range(struct bitmap *map,uint32_t lo, uint32_t hi){
    assert(map);
    assert(hi < map->length);
	assert(lo <= hi);

    if(lo/8<hi/8){
        map->data[lo/8] &= ~msbmask[lo%8];
        uint32_t i = lo/8+1;
        for(;i<hi/8;i++){
            map->data[i] = 0;
        }
        map->data[hi/8] &= ~lsbmask[hi%8];
    }else{
        map->data[lo/8] &= ~(msbmask[lo%8]&lsbmask[hi%8]);
    }
}

void 
bitmap_clear_bit_all(struct bitmap *map){
    assert(map);
    uint32_t a = (map->length/8)/8;
    uint32_t b = (map->length/8)%8;
    uint32_t r = map->length%8;

    uint32_t i=0;
    for(;i<a;i++){
        ((uint64_t*)(map->data))[i] = 0;
    }
    for(i=0;i<b;i++){
        map->data[a*8+i] = 0;
    }
    if(r!=0){
        map->data[a*8 + b] = 0;
    }
}

uint32_t
bitmap_get_first_clear_bit(struct bitmap *map){
    assert(map);
    uint32_t a = (map->length/8)/8;
    uint32_t b = (map->length/8)%8;
    uint32_t r = map->length%8;
    uint32_t i =0,j=0;
    for(;i<a;i++){
        if( ((uint64_t*)(map->data))[i]!=UINT64_MAX ){
            for(;j<8;j++){
                if( (map->data)[i*8+j]!=UINT8_MAX ){
                    uint8_t tmp = (map->data)[i*8+j];
                    uint32_t k = ((tmp&0x0f) != 0x0f) ? firstbit[tmp&0x0f] : firstbit[ (tmp&0xf0)>>4 ] + 4;
                    return (i*64 + j*8 + k);
                }
            }
        }
    }
    for(j=0;j<b;j++){
        if( (map->data)[a*8+j]!=UINT8_MAX ){
            uint8_t tmp = (map->data)[a*8+j];
            uint32_t k = ((tmp&0x0f) != 0x0f) ? firstbit[tmp&0x0f] : firstbit[ (tmp&0xf0)>>4 ] + 4;
            return (a*64 + j*8 + k);
        }
    }
    for(j=0;j<r;j++){
        uint8_t tmp = (map->data)[a*8+b];
        if( (~tmp) & (1<<j) ){
            return (a*64 + b*8 + j);
        }
    }
    return UINT32_MAX;
}