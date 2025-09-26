# _hash_finish_split

## Location
[src/backend/access/hash/hashpage.c:1356-1473](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L1356-L1473)

## Overview
Completes a previously interrupted hash table bucket split operation by building a hash table of TIDs from the new bucket and performing the actual split.

## Definition

```c
void
_hash_finish_split(Relation rel, Buffer metabuf, Buffer obuf, Bucket obucket,
				   uint32 maxbucket, uint32 highmask, uint32 lowmask)
```
## Detailed Description
This function is responsible for completing a bucket split operation that was previously interrupted. It works by:

1. Creating an in-memory hash table to track TIDs (tuple identifiers) that have already been moved to the new bucket
2. Scanning the new bucket and all its overflow pages to populate this TID hash table
3. Attempting to acquire cleanup locks on both old and new buckets
4. If locks are successfully acquired, calling  to complete the split operation using the TID hash table to skip already-moved tuples

The function handles the case where a split operation was interrupted (e.g., due to a crash) and needs to be completed. The TID hash table ensures that tuples already moved to the new bucket are not processed again during the completion of the split.

## Parameters / Member Variables
- : The hash index relation being operated on
- : Buffer containing the metapage (must be pinned but not locked)
- : Buffer containing the old bucket's primary page (must be pinned but not locked)
- : The bucket number of the old bucket being split
- : The current maximum bucket number in the hash table
- : High-order bits mask for hash value calculation
- : Low-order bits mask for hash value calculation

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](hash_create.md) (creates the TID tracking hash table)
  - [_hash_get_newblock_from_oldbucket](_hash_get_newblock_from_oldbucket.md) (gets the new bucket's block number)
  - [_hash_getbuf](_hash_getbuf.md) (reads bucket pages)
  - [hash_search](hash_search.md) (inserts TIDs into the tracking hash table)
  - [ConditionalLockBufferForCleanup](../C/ConditionalLockBufferForCleanup.md) (attempts to acquire cleanup locks)
  - [_hash_splitbucket](_hash_splitbucket.md) (performs the actual split operation)
  - [_hash_dropbuf](_hash_dropbuf.md) (releases buffer)
  - [hash_destroy](hash_destroy.md) (cleans up the TID hash table)
- Called from (representative examples):
  - [_hash_doinsert](_hash_doinsert.md) (during tuple insertion when split completion is needed)
  - [_hash_expandtable](_hash_expandtable.md) (during table expansion operations)

## Notes and Other Information
- The function uses conditional locking to avoid blocking - if cleanup locks cannot be acquired, it silently gives up and the split will be retried on the next insertion
- The TID hash table is created in the current memory context and is destroyed before the function returns
- The function maintains pins on the metapage and old bucket buffers as required by the caller
- This is part of PostgreSQL's hash index implementation for handling interrupted split operations gracefully
- The function handles both the primary bucket page and any overflow pages in the new bucket