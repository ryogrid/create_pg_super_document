# BloomFilter

## Location
src/backend/access/brin/brin_bloom.c: 242 - 257

## Overview
BloomFilter represents a bloom filter data structure used in BRIN indexes, built on hashes of indexed values to provide efficient membership testing with controlled false positive rates.

## Definition
```c
typedef struct BloomFilter
{
    /* varlena header (do not touch directly!) */
    int32       vl_len_;

    /* space for various flags (unused for now) */
    uint16      flags;

    /* fields for the HASHED phase */
    uint8       nhashes;        /* number of hash functions */
    uint32      nbits;          /* number of bits in the bitmap (size) */
    uint32      nbits_set;      /* number of bits set to 1 */

    /* data of the bloom filter */
    char        data[FLEXIBLE_ARRAY_MEMBER];
} BloomFilter;
```

## Detailed Description
BloomFilter implements a probabilistic data structure that efficiently tests set membership with a known false positive rate but no false negatives. The structure computes uint32 hashes of indexed values and stores these hashes in a bitmap. The implementation supports multiple hash functions to reduce collision probability and includes tracking of set bits to monitor filter saturation. The filter can potentially be enhanced with sparse storage (keeping only non-zero bytes) or adaptive behavior (stopping usage when false positive rate becomes too high), though current implementation relies on varlena compression for space efficiency.

## Parameters / Member Variables
- `vl_len_`: Standard varlena header for PostgreSQL variable-length data types
- `flags`: Reserved space for future feature flags (currently unused)
- `nhashes`: Number of hash functions used by the bloom filter algorithm
- `nbits`: Total number of bits in the bloom filter bitmap, determining filter size
- `nbits_set`: Count of bits currently set to 1, used for monitoring filter saturation
- `data`: Flexible array member containing the actual bloom filter bitmap data

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - bloom_filter_size
  - bloom_init
  - bloom_add_value
  - bloom_contains_value
  - brin_bloom_add_value
  - brin_bloom_consistent
  - brin_bloom_union
  - brin_bloom_summary_out

## Notes and Other Information
Located in src/backend/access/brin/brin_bloom.c:242-257. The structure includes extensive comments about potential optimizations like sparse bloom filters and adaptive behavior based on false positive rates. The implementation balances memory efficiency with query performance, utilizing PostgreSQL's compression capabilities rather than implementing custom sparse storage.