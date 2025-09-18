# _hash_get_oldblock_from_newbucket

## Location
src/backend/access/hash/hashutil.c: 422 - 460

## Overview
Gets the block number of the original bucket from which a new bucket is being split during hash table expansion.

## Definition
BlockNumber _hash_get_oldblock_from_newbucket(Relation rel, Bucket new_bucket)

## Detailed Description
This function determines the block number of the old bucket that corresponds to a new bucket during a hash table split operation. When a hash index grows, buckets are split and tuples are redistributed. This function calculates which original bucket a new bucket was derived from by using a mask operation.

The key insight is that during bucket splitting, the old bucket can be found by masking the most significant bit of the new bucket number. The function constructs this mask by finding the leftmost 1 bit in the new bucket number and creating a mask that covers all bits to the right of it. This effectively maps the new bucket back to its original bucket in the lower half of the hash table.

The function reads the hash meta page to access the bucket-to-block mapping information and converts the calculated old bucket number to its corresponding physical block number.

## Parameters / Member Variables
- `rel`: The hash index relation
- `new_bucket`: The bucket number of the new bucket being processed

## Dependencies
- Functions called/Symbols referenced:
  - pg_leftmost_one_pos32
  - _hash_getbuf
  - HashPageGetMeta
  - BufferGetPage
  - BUCKET_TO_BLKNO
  - _hash_relbuf
- Referenced types/constants:
  - Bucket
  - HashMetaPage
  - HASH_METAPAGE
  - HASH_READ
  - LH_META_PAGE
- Called from (representative examples):
  - _hash_first

## Notes and Other Information
The function is critical for hash index splitting operations. It cannot rely on the current hashm_lowmask value stored in the meta page because it needs the mask value that was prevalent when the bucket split started. The mask calculation using pg_leftmost_one_pos32 ensures correct mapping from new buckets to their corresponding old buckets during the split process. This is essential for maintaining data consistency during hash table expansion.