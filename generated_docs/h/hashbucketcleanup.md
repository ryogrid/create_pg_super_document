# hashbucketcleanup

## Location
[src/backend/access/hash/hash.c:687-927](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash.c#L687-L927)

## Overview
Performs deletion of index entries from a specific hash bucket, handling both regular tuple deletion via callback and cleanup of tuples moved by bucket split operations.

## Definition


## Detailed Description
The hashbucketcleanup function is a comprehensive helper function that performs the actual tuple deletion work for a single hash bucket. It operates on the entire bucket chain, including all overflow pages, systematically scanning each tuple and determining whether it should be deleted based on either callback criteria (for regular VACUUM operations) or split cleanup requirements.

The function implements a sophisticated locking protocol to prevent concurrent scans from interfering with the cleanup process. It uses lock chaining, where it locks the next page in the bucket chain before releasing the lock on the previous page. This ensures that no concurrent scan can pass the cleanup scan and potentially see a tuple that is about to be deleted.

For split cleanup operations, the function identifies tuples that were moved to other buckets during split operations but remain in the original bucket. These tuples are marked for deletion to maintain hash index consistency. The function also handles clearing the garbage flag from buckets after split cleanup is complete.

The function implements WAL logging for all modifications, ensuring crash recovery consistency. After deletion operations are complete, it attempts to squeeze the bucket to compact free space, but only when a cleanup lock can be obtained without blocking.

## Parameters / Member Variables
- : The hash index relation being cleaned up
- : The bucket number being processed
- : Buffer containing the primary bucket page
- : Block number of the primary bucket page
- : Buffer access strategy for the operation
- : Maximum bucket number in the hash index
- : High-order bits mask for hash bucket calculation
- : Low-order bits mask for hash bucket calculation
- : Pointer to counter for tracking number of tuples removed
- : Pointer to counter for tracking total number of tuples
- : Boolean flag indicating whether to perform split cleanup
- : Function pointer for determining which tuples to delete
- : Opaque state data passed to the callback function

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_get_newbucket_from_oldbucket](_hash_get_newbucket_from_oldbucket.md)
  - [vacuum_delay_point](../v/vacuum_delay_point.md)
  - HashPageGetOpaque
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - [PageGetItem](../P/PageGetItem.md)
  - [PageGetItemId](../P/PageGetItemId.md)
  - [_hash_hashkey2bucket](_hash_hashkey2bucket.md)
  - _hash_get_indextuple_hashkey
  - [PageIndexMultiDelete](../P/PageIndexMultiDelete.md)
  - H_HAS_DEAD_TUPLES
  - RelationNeedsWAL
  - [XLogInsert](../X/XLogInsert.md)
  - [_hash_getbuf_with_strategy](_hash_getbuf_with_strategy.md)
  - [_hash_relbuf](_hash_relbuf.md)
  - IsBufferCleanupOK
  - [_hash_squeezebucket](_hash_squeezebucket.md)
- Called from (representative examples):
  - [hashbulkdelete](hashbulkdelete.md)
  - [_hash_expandtable](_hash_expandtable.md)
  - [_hash_splitbucket](_hash_splitbucket.md)

## Notes and Other Information
- Expects caller to hold cleanup lock on primary bucket page and returns with write lock held
- Uses lock chaining to prevent concurrent scans from interfering with cleanup process
- Retains pin on primary bucket page throughout the operation to prevent concurrent splits
- Handles both regular tuple deletion (via callback) and split cleanup operations
- Clears LH_PAGE_HAS_DEAD_TUPLES flag when removing dead tuples from pages
- Clears LH_BUCKET_NEEDS_SPLIT_CLEANUP flag after completing split cleanup
- Implements comprehensive WAL logging for crash recovery consistency
- Attempts bucket squeezing at the end if cleanup lock is available and deletions occurred
- Uses vacuum_delay_point() to allow vacuum throttling during long operations