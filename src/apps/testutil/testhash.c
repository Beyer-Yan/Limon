#include "kvutil.h"
#include <stdio.h>

int test_hash(uint64_t key,int numBuckets) {
  int64_t b  = -1;
  int64_t j = 0;;

  for(;j<numBuckets;){
    b = j;
    key = key*2862933555777941757 + 1;
    j = (int64_t)( (double)(b+1)  * ((double)( (int64_t)1<<31) / (double)((key>>33)+1) ) );
  }
  return (int32_t)b;
}

int
main(int argc, char **argv){
    uint64_t i = 1;
    for(;i<10000;i++){
        int res = test_hash(i,8);
        printf("hash:%d\n",res);
    }
}