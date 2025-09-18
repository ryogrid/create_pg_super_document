# _hash_expandtable

## Location
src/backend/access/hash/hashpage.c: 614 - 991

## Overview
Attempts to expand the hash table by creating one new bucket, handling the complex process of bucket splitting with proper locking and crash recovery support.

## Definition
```c
void _hash_expandtable(Relation rel, Buffer metabuf)
```

## Detailed Description
This function implements the core hash table expansion algorithm that creates a new bucket and redistributes tuples from an existing bucket. It performs comprehensive checks before proceeding with the split, including verifying that a split is still needed, handling any pending splits or cleanup operations, and ensuring proper locking. The function maintains crash safety through WAL logging and uses a restart mechanism to handle concurrent operations. If the split point increases, it allocates new bucket pages in batches. The actual tuple redistribution is delegated to _hash_splitbucket after updating metadata and marking buckets appropriately.

## Parameters / Member Variables
- `rel`: The hash index relation being expanded
- `metabuf`: Buffer containing the metadata page (caller must hold pin but no lock)

## Dependencies
- Functions called/Symbols referenced:
  - _hash_checkpage
  - HashPageGetMeta
  - BUCKET_TO_BLKNO
  - [_hash_getbuf_with_condlock_cleanup](_hash_getbuf_with_condlock_cleanup.md)
  - [_hash_finish_split](_hash_finish_split.md)
  - [_hash_dropbuf](_hash_dropbuf.md)
  - [hashbucketcleanup](hashbucketcleanup.md)
  - [_hash_spareindex](_hash_spareindex.md)
  - _hash_get_totalbuckets
  - [_hash_alloc_buckets](_hash_alloc_buckets.md)
  - [_hash_getnewbuf](_hash_getnewbuf.md)
  - [_hash_splitbucket](_hash_splitbucket.md)
  - Various WAL logging functions (XLogInsert, XLogRegisterBuffer, etc.)
- Called from (representative examples):
  - [_hash_doinsert](_hash_doinsert.md)

## Notes and Other Information
- Silently does nothing if cleanup locks cannot be acquired on old or new buckets
- Uses restart mechanism to handle concurrent splits and cleanup operations
- Maintains strict upper limit of 0x7FFFFFFE buckets to prevent overflow
- Implements comprehensive WAL logging for crash recovery
- Updates metadata including bucket masks and overflow point when creating new splitpoints
- Uses critical sections to ensure atomic updates of shared buffer pages
- Handles both simple bucket splits and complex splitpoint increases requiring batch allocation