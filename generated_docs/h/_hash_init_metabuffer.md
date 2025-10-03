# _hash_init_metabuffer

## Location
[src/backend/access/hash/hashpage.c:498-595](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L498-L595)

## Overview
Initializes the metadata page of a hash index with appropriate configuration based on estimated tuple count and fill factor.

## Definition

```c
void
_hash_init_metabuffer(Buffer buf, double num_tuples, RegProcedure procid,
					  uint16 ffactor, bool initpage)
```
## Detailed Description
This function sets up the metadata page for a hash index, which is the control structure that manages the overall state of the hash index. It calculates the initial number of buckets based on the estimated tuple count and fill factor, initializes the page structure, and sets up the metadata fields including magic numbers, version information, bucket configuration, and bitmap management parameters. The function ensures proper page layout by setting pd_lower to prevent metadata loss during WAL compression.

## Parameters / Member Variables
- : Buffer containing the metadata page to be initialized
- : Estimated number of tuples that will be stored in the index
- : OID of the primary hash support function for forensic purposes
- : Fill factor determining how full buckets should be before splitting
- : Whether to initialize the page structure itself

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_get_totalbuckets](_hash_get_totalbuckets.md)
  - [_hash_spareindex](_hash_spareindex.md)
  - [_hash_pageinit](_hash_pageinit.md)
  - [BufferGetPage](../B/BufferGetPage.md)
  - [BufferGetPageSize](../B/BufferGetPageSize.md)
  - HashPageGetOpaque
  - HashPageGetMeta
  - HashGetMaxBitmapSize
  - [pg_leftmost_one_pos32](../p/pg_leftmost_one_pos32.md)
  - [pg_nextpower2_32](../p/pg_nextpower2_32.md)
  - MemSet
- Called from (representative examples):
  - [hash_xlog_init_meta_page](hash_xlog_init_meta_page.md)
  - [_hash_init](_hash_init.md)

## Notes and Other Information
- Always forces at least 2 bucket pages regardless of calculated requirements
- Upper limit of 0x40000000 buckets to prevent overflow issues
- Sets up initial spare page mapping for future table expansion
- Initializes bitmap size and shift parameters for efficient bitmap operations
- Critical for proper WAL recovery through pd_lower setting

## Simplified Source

```c
void _hash_init_metabuffer(Buffer buf, double num_tuples, RegProcedure procid,
                          uint16 ffactor, bool initpage)
{
    HashMetaPage metap;
    HashPageOpaque pageopaque;
    Page page;
    uint32 num_buckets, spare_index;

    // Calculate initial bucket count based on fill factor
    double dnumbuckets = num_tuples / ffactor;
    if (dnumbuckets <= 2.0)
        num_buckets = 2;
    else if (dnumbuckets >= (double) 0x40000000)
        num_buckets = 0x40000000;
    else
        num_buckets = _hash_get_totalbuckets(_hash_spareindex(dnumbuckets));

    spare_index = _hash_spareindex(num_buckets);

    page = BufferGetPage(buf);
    if (initpage)
        _hash_pageinit(page, BufferGetPageSize(buf));

    // Initialize page opaque data
    pageopaque = HashPageGetOpaque(page);
    pageopaque->hasho_prevblkno = InvalidBlockNumber;
    pageopaque->hasho_nextblkno = InvalidBlockNumber;
    pageopaque->hasho_bucket = InvalidBucket;
    pageopaque->hasho_flag = LH_META_PAGE;
    pageopaque->hasho_page_id = HASHO_PAGE_ID;

    // Initialize metadata
    metap = HashPageGetMeta(page);
    metap->hashm_magic = HASH_MAGIC;
    metap->hashm_version = HASH_VERSION;
    metap->hashm_ntuples = 0;
    metap->hashm_nmaps = 0;
    metap->hashm_ffactor = ffactor;
    metap->hashm_bsize = HashGetMaxBitmapSize(page);

    // Calculate bitmap parameters
    uint32 lshift = pg_leftmost_one_pos32(metap->hashm_bsize);
    metap->hashm_bmsize = 1 << lshift;
    metap->hashm_bmshift = lshift + BYTE_TO_BIT;

    metap->hashm_procid = procid;
    metap->hashm_maxbucket = num_buckets - 1;

    // Set bucket masks for hash-to-bucket mapping
    metap->hashm_highmask = pg_nextpower2_32(num_buckets + 1) - 1;
    metap->hashm_lowmask = (metap->hashm_highmask >> 1);

    // Initialize arrays
    MemSet(metap->hashm_spares, 0, sizeof(metap->hashm_spares));
    MemSet(metap->hashm_mapp, 0, sizeof(metap->hashm_mapp));

    // Set up initial splitpoint mapping
    metap->hashm_spares[spare_index] = 1;
    metap->hashm_ovflpoint = spare_index;
    metap->hashm_firstfree = 0;

    // Set pd_lower to preserve metadata during WAL compression
    ((PageHeader) page)->pd_lower =
        ((char *) metap + sizeof(HashMetaPageData)) - (char *) page;
}
```