# _hash_getbucketbuf_from_hashkey

## Location
[src/backend/access/hash/hashpage.c:1559-1617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L1559-L1617)

## Overview
Retrieves the appropriate bucket buffer for a given hash key, using cached metapage data to optimize performance and handling bucket splits.

## Definition


## Detailed Description
This function efficiently locates and returns the correct bucket buffer for a given hash key. It implements several optimizations:

1. **Cached Metapage Usage**: Uses cached metapage data to avoid reading the metapage from disk on every operation, reducing buffer manager traffic and contention
2. **Split Handling**: Implements a retry loop to handle bucket splits that may occur between cache reads
3. **Bucket Calculation**: Computes the target bucket using the hash key and metapage parameters
4. **Lock Management**: Properly acquires and manages locks based on the requested access type

The function works by:
- Getting cached metapage data
- Computing the target bucket number from the hash key
- Converting bucket number to block number and fetching the bucket page
- Verifying the bucket hasn't been split (by checking hasho_prevblkno)
- If bucket was split, refreshing the metapage cache and retrying

## Parameters / Member Variables
- : The hash index relation being accessed
- : The hash key value used to determine the target bucket
- : Access type (HASH_READ or HASH_WRITE) indicating required lock type
- : Output parameter containing the metapage data used for mapping (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [_hash_getcachedmetap](_hash_getcachedmetap.md) (gets cached metapage data)
  - [_hash_hashkey2bucket](_hash_hashkey2bucket.md) (converts hash key to bucket number)
  - BUCKET_TO_BLKNO (converts bucket number to block number)
  - [_hash_getbuf](_hash_getbuf.md) (reads bucket page from disk)
  - HashPageGetOpaque (gets hash-specific page data)
  - [_hash_relbuf](_hash_relbuf.md) (releases buffer with lock)
  - [_hash_dropbuf](_hash_dropbuf.md) (releases buffer with pin)
  - HASH_READ, HASH_WRITE (access mode constants)
  - LH_BUCKET_PAGE (lock mode for bucket pages)
- Called from (representative examples):
  - [_hash_doinsert](_hash_doinsert.md) (during tuple insertion operations)
  - [_hash_first](_hash_first.md) (during index scan initialization)

## Notes and Other Information
- The function assumes bucket pages do not move or get removed once allocated, enabling the caching optimization
- The retry loop handles the race condition where a bucket split occurs between reading cached metapage data and accessing the bucket
- The split detection works by comparing hasho_prevblkno with the cached maxbucket value
- If cachedmetap parameter is provided, it receives the metapage data used for the operation, which some callers need for accessing old buckets during splits
- This function is critical for hash index performance as it's called for most bucket access operations
- The access parameter must be either HASH_READ or HASH_WRITE - this is enforced by assertion
- Buffer management is carefully handled to avoid leaks, with proper cleanup of metapage buffers