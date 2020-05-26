#include "kvutil.h"
int
main(int argc, char **argv){
    int i = 1;
    for(;i<10000;i++){
        uint32_t res = kv_hash(&i,4,4);
        printf("hash:%d\n",res);
    }
}