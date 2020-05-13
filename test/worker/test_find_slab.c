#include <stdio.h>
static int slab_sizes[] = { 47, 56, 65, 73, 89, 93, 105, 124, 146, 195, 227, 256, 273, 292, 315, 341, 
                     372, 409, 455, 512, 585, 682, 819, 1024, 1365, 2048,4096,4608,5376,6144,
                     7168,8192,9216,10752,12288,14336,16384,18432,21504,24576,28672,32256,36864,
                     43008,49152,57344,64512,73728,77824, 86016, 94208, 102400, 110592, 122880, 
                     135168, 147456, 163840, 180224,196608, 217088, 237568, 262144, 290816, 
                     319488, 352256, 389120, 430080, 475136, 524288, 581632, 643072, 712704, 
                     790528, 876544, 970752, 1077248, 1196032, 1327104, 1474560, 1638400, 1818624,
                     2019328, 2240512, 2486272, 2760704, 3063808, 3403776, 3780608, 4194304
};

static int _find_slab(int item_size){
    int len = sizeof(slab_sizes)/sizeof(slab_sizes[0]);
    int i = 0;
    int j = len-1;
    int mid = (i+j)/2;

    if (item_size<slab_sizes[0] || item_size>slab_sizes[len-1]){
        return -1;
    }

    while(1){
        if(slab_sizes[mid]<=item_size && slab_sizes[mid+1]>=item_size){
            return slab_sizes[mid]==item_size?mid:mid+1;
        }else if(slab_sizes[mid]>item_size){
            j = mid;
        }else{
            i = mid+1;
        }
        mid = (i+j)/2;
    }
}

void main(){
    int res;

    res = _find_slab(47);
    printf("res:%d, expected:%d\n",res,slab_sizes[res]);

    res = _find_slab(4194304);
    int len = sizeof(slab_sizes)/sizeof(slab_sizes[0]);
    printf("res:%d, expected:%d\n",res,slab_sizes[res]);

    res = _find_slab(1196035);
    printf("res:%d, expected:%d\n",res,slab_sizes[res]);

    res = _find_slab(430080+5);
    printf("res:%d, expected:%d\n",res,slab_sizes[res]);

    res = _find_slab(10);
    printf("res:%d, expected:%d\n",res,-1);

    res = _find_slab(4194305);
    printf("res:%d, expected:%d\n",res,-1);
}
