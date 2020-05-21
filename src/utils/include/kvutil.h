#ifndef KVS_UTIL_H
#define KVS_UTIL_H
#include <stdint.h>

#define CACHE_LINE_LENGTH 64

//The align parameeter must be a number of power of two
#define KV_ALIGN(size,align)	(((size)+(align)-1)&~((align)-1))

#define NOP10() asm("nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;nop;")

//For x86 only
#define rdtscll(val) {                                           \
       unsigned int __a,__d;                                        \
       asm volatile("rdtsc" : "=a" (__a), "=d" (__d));              \
       (val) = ((unsigned long)__a) | (((unsigned long)__d)<<32);   \
}

#define declare_periodic_count \
      uint64_t __real_start = 0, __start, __last, __nb_count; \
      if(!__real_start) { \
         rdtscll(__real_start); \
         __start = __real_start; \
         __nb_count = 0; \
      }

#define periodic_count(period, msg, args...) \
   do { \
      rdtscll(__last); \
      __nb_count++; \
      if(cycles_to_us(__last - __start) > ((period)*1000LU)) { \
         printf("(%s,%d) [%3lus] [%7lu ops/s] " msg "\n", __FUNCTION__ , __LINE__, cycles_to_us(__last - __real_start)/1000000LU, __nb_count*1000000LU/cycles_to_us(__last - __start), ##args); \
         __nb_count = 0; \
         __start = __last; \
      } \
   } while(0);


uint32_t kv_hash(const uint8_t* key, uint32_t key_len, uint32_t num_buckets);
uint64_t kv_cycles_to_us(uint64_t cycles);
void     kv_shuffle(uint64_t *array, uint64_t n);

#endif
