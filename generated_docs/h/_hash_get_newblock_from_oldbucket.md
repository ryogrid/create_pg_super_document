# _hash_get_newblock_from_oldbucket

## Location
src/backend/access/hash/hashutil.c: 461 - 493

## Overview
Gets the block number of a new bucket that will be generated after splitting from an old bucket during hash table expansion.

## Definition
BlockNumber _hash_get_newblock_from_oldbucket(Relation rel, Bucket old_bucket)

## Detailed Description
This function determines the block number of the new bucket that will be created when an old bucket is split during hash table expansion. It is primarily used to complete incomplete split operations where the system needs to locate the new bucket that corresponds to a given old bucket.

The function works by first reading the hash meta page to get the current table parameters (lowmask and maxbucket), then calling _hash_get_newbucket_from_oldbucket to calculate the new bucket number, and finally converting that bucket number to its corresponding physical block number using the bucket-to-block mapping.

This is particularly important for handling incomplete splits, where the system can guarantee that at most one bucket split could be in progress from any given old bucket, making the mapping deterministic.

## Parameters / Member Variables
- `rel`: The hash index relation
- `old_bucket`: The bucket number of the old bucket being split

## Dependencies
- Functions called/Symbols referenced:
  - _hash_getbuf
  - HashPageGetMeta
  - BufferGetPage
  - _hash_get_newbucket_from_oldbucket
  - BUCKET_TO_BLKNO
  - _hash_relbuf
- Referenced types/constants:
  - Bucket
  - HashMetaPage
  - HASH_METAPAGE
  - HASH_READ
  - LH_META_PAGE
- Called from (representative examples):
  - _hash_finish_split

## Notes and Other Information
This function is essential for completing incomplete hash index splits. It relies on the current table state (lowmask and maxbucket) from the meta page and delegates the actual bucket number calculation to _hash_get_newbucket_from_oldbucket. The function is designed with the assumption that only one bucket split can be in progress from any old bucket at a time, which simplifies the logic for determining the corresponding new bucket.