#ifndef KVS_BLOOMFILTER_H__
#define KVS_MICRO_BLOOMFILTER_H__

/**
 *  Simple implementation of in-memory BloomFilter_x64, 
 *  reference -> https://github.com/upbit/bloomfilter
**/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#define __MGAIC_CODE__          (0x01464C42)

#define FORCE_INLINE __attribute__((always_inline))

#define BYTE_BITS           (8)
#define MIX_UINT64(v)       ((uint32_t)((v>>32)^(v)))

#define SETBIT(filter, n)   (filter->pstFilter[n/BYTE_BITS] |= (1 << (n%BYTE_BITS)))
#define GETBIT(filter, n)   (filter->pstFilter[n/BYTE_BITS] & (1 << (n%BYTE_BITS)))

typedef struct
{
    uint8_t cInitFlag; 
    uint8_t cResv[3];

    uint32_t dwMaxItems;
    double dProbFalse;
    uint32_t dwFilterBits;
    uint32_t dwHashFuncs;
    
    uint32_t dwSeed;  
    uint32_t dwCount;

    uint32_t dwFilterSize; 
    unsigned char *pstFilter;
    uint32_t *pdwHashPos;
} BaseBloomFilter;

static inline void _CalcBloomFilterParam(uint32_t n, double p, uint32_t *pm, uint32_t *pk)
{
    /**
     *  n - Number of items in the filter
     *  p - Probability of false positives, float between 0 and 1 or a number indicating 1-in-p
     *  m - Number of bits in the filter
     *  k - Number of hash functions
     *
     *  f = ln(2) × ln(1/2) × m / n = (0.6185) ^ (m/n)
     *  m = -1 * ln(p) × n / 0.6185
     *  k = ln(2) × m / n = 0.6931 * m / n
    **/

    uint32_t m, k;

    m = (uint32_t) ceil(-1 * log(p) * n / 0.6185);
    m = (m - m % 64) + 64;  
    k = (uint32_t) (0.6931 * m / n);
    k++;

    *pm = m;
    *pk = k;
    return;
}

static inline int InitBloomFilter(BaseBloomFilter *pstBloomfilter, uint32_t dwSeed, uint32_t dwMaxItems, double dProbFalse)
{
    if (pstBloomfilter == NULL)
        return -1;
    if ((dProbFalse <= 0) || (dProbFalse >= 1))
        return -2;

    if (pstBloomfilter->pstFilter != NULL)
        free(pstBloomfilter->pstFilter);
    if (pstBloomfilter->pdwHashPos != NULL)
        free(pstBloomfilter->pdwHashPos);

    memset(pstBloomfilter, 0, sizeof(BaseBloomFilter));

    pstBloomfilter->dwMaxItems = dwMaxItems;
    pstBloomfilter->dProbFalse = dProbFalse;
    pstBloomfilter->dwSeed = dwSeed;

    _CalcBloomFilterParam(pstBloomfilter->dwMaxItems, pstBloomfilter->dProbFalse,
                    &pstBloomfilter->dwFilterBits, &pstBloomfilter->dwHashFuncs);

    pstBloomfilter->dwFilterSize = pstBloomfilter->dwFilterBits / BYTE_BITS;
    pstBloomfilter->pstFilter = (unsigned char *) malloc(pstBloomfilter->dwFilterSize);
    if (NULL == pstBloomfilter->pstFilter)
        return -100;

    pstBloomfilter->pdwHashPos = (uint32_t*) malloc(pstBloomfilter->dwHashFuncs * sizeof(uint32_t));
    if (NULL == pstBloomfilter->pdwHashPos)
        return -200;

    //printf(">>> Init BloomFilter(n=%u, p=%f, m=%u, k=%d), malloc() size=%.2fMB\n",
    //        pstBloomfilter->dwMaxItems, pstBloomfilter->dProbFalse, pstBloomfilter->dwFilterBits,
    //        pstBloomfilter->dwHashFuncs, (double)pstBloomfilter->dwFilterSize/1024/1024);
    
    memset(pstBloomfilter->pstFilter, 0, pstBloomfilter->dwFilterSize);
    pstBloomfilter->cInitFlag = 1;
    return 0;
}

static inline int FreeBloomFilter(BaseBloomFilter *pstBloomfilter)
{
    if (pstBloomfilter == NULL)
        return -1;
    
    pstBloomfilter->cInitFlag = 0;
    pstBloomfilter->dwCount = 0;

    free(pstBloomfilter->pstFilter);
    pstBloomfilter->pstFilter = NULL;
    free(pstBloomfilter->pdwHashPos);
    pstBloomfilter->pdwHashPos = NULL;
    return 0;
}

static inline int ResetBloomFilter(BaseBloomFilter *pstBloomfilter)
{
    if (pstBloomfilter == NULL)
        return -1;
    
    pstBloomfilter->cInitFlag = 0;
    pstBloomfilter->dwCount = 0;
    return 0;
}

static inline int RealResetBloomFilter(BaseBloomFilter *pstBloomfilter)
{
    if (pstBloomfilter == NULL)
        return -1;
    
    memset(pstBloomfilter->pstFilter, 0, pstBloomfilter->dwFilterSize);
    pstBloomfilter->cInitFlag = 1;
    pstBloomfilter->dwCount = 0;
    return 0;
}

// MurmurHash2, 64-bit versions, by Austin Appleby
// https://sites.google.com/site/murmurhash/
static FORCE_INLINE uint64_t MurmurHash2_x64 ( const void * key, int len, uint32_t seed )
{
	const uint64_t m = 0xc6a4a7935bd1e995;
	const int r = 47;

	uint64_t h = seed ^ (len * m);

	const uint64_t * data = (const uint64_t *)key;
	const uint64_t * end = data + (len/8);

	while(data != end)
	{
		uint64_t k = *data++;

		k *= m; 
		k ^= k >> r; 
		k *= m; 
		
		h ^= k;
		h *= m;
	}

	const uint8_t * data2 = (const uint8_t*)data;

	switch(len & 7)
	{
	case 7: h ^= ((uint64_t)data2[6]) << 48;
	case 6: h ^= ((uint64_t)data2[5]) << 40;
	case 5: h ^= ((uint64_t)data2[4]) << 32;
	case 4: h ^= ((uint64_t)data2[3]) << 24;
	case 3: h ^= ((uint64_t)data2[2]) << 16;
	case 2: h ^= ((uint64_t)data2[1]) << 8;
	case 1: h ^= ((uint64_t)data2[0]);
	        h *= m;
	};
 
	h ^= h >> r;
	h *= m;
	h ^= h >> r;

	return h;
}

static FORCE_INLINE void bloom_hash(BaseBloomFilter *pstBloomfilter, const void * key, int len)
{
    //if (pstBloomfilter == NULL) return;
    int i;
    uint32_t dwFilterBits = pstBloomfilter->dwFilterBits;
    uint64_t hash1 = MurmurHash2_x64(key, len, pstBloomfilter->dwSeed);
    uint64_t hash2 = MurmurHash2_x64(key, len, MIX_UINT64(hash1));
    
    for (i = 0; i < (int)pstBloomfilter->dwHashFuncs; i++)
    {
        pstBloomfilter->pdwHashPos[i] = (hash1 + i*hash2) % dwFilterBits;
    }

    return;
}

static FORCE_INLINE int BloomFilter_Add(BaseBloomFilter *pstBloomfilter, const void * key, int len)
{
    if ((pstBloomfilter == NULL) || (key == NULL) || (len <= 0))
        return -1;
    
    int i;
    
    if (pstBloomfilter->cInitFlag != 1)
    {
        memset(pstBloomfilter->pstFilter, 0, pstBloomfilter->dwFilterSize);
        pstBloomfilter->cInitFlag = 1;
    }
    
    bloom_hash(pstBloomfilter, key, len);
    for (i = 0; i < (int)pstBloomfilter->dwHashFuncs; i++)
    {
        SETBIT(pstBloomfilter, pstBloomfilter->pdwHashPos[i]);
    }
    
    pstBloomfilter->dwCount++;
    if (pstBloomfilter->dwCount <= pstBloomfilter->dwMaxItems)
        return 0;
    else
        return 1; 
}

// resturn:0-exist，1-not exist，negtive-failure
static FORCE_INLINE int BloomFilter_Check(BaseBloomFilter *pstBloomfilter, const void * key, int len)
{
    if ((pstBloomfilter == NULL) || (key == NULL) || (len <= 0))
        return -1;
    
    int i;
    
    bloom_hash(pstBloomfilter, key, len);
    for (i = 0; i < (int)pstBloomfilter->dwHashFuncs; i++)
    {
        if (GETBIT(pstBloomfilter, pstBloomfilter->pdwHashPos[i]) == 0)
            return 1;
    }
    
    return 0;
}

#endif