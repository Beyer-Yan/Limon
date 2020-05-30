#ifndef RANDOM_H
#define RANDOM_H 1

typedef long (*random_gen_t)(void);

unsigned long xorshf96(void);
unsigned long locxorshf96(void);

// must be called after each thread creation
void init_seed(void); 
void init_zipf_generator(long min, long max);

// zipf distribution, call init_zipf_generator first
long zipf_next(void); 

// uniform, call init_zipf_generator first
long uniform_next(void); 

 // returns something between 1 and 1000
long bogus_rand(void);

// production workload simulator
long production_random1(void); 

// production workload simulator
long production_random2(void); 

const char *get_function_name(random_gen_t f);
#endif
