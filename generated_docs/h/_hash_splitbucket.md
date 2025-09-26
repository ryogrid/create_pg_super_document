# _hash_splitbucket

## Location
[src/backend/access/hash/hashpage.c:1073-1355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashpage.c#L1073-L1355)

## Overview
Partitions tuples between old and new buckets during hash table expansion, handling the core redistribution logic with support for incomplete split recovery.

## Definition
```c
static void _hash_splitbucket(Relation rel, Buffer metabuf, Bucket obucket, Bucket nbucket,
                             Buffer obuf, Buffer nbuf, HTAB *htab, uint32 maxbucket,
                             uint32 highmask, uint32 lowmask)
```

## Detailed Description
This function implements the core tuple redistribution algorithm for hash bucket splitting. It scans through all pages in the old bucket's overflow chain, determines which tuples belong in the new bucket based on their hash values, and moves appropriate tuples while marking them with INDEX_MOVED_BY_SPLIT_MASK. The function handles overflow page allocation for the new bucket when needed, supports recovery from incomplete splits via the htab parameter, and implements predicate lock copying for serializable isolation. After tuple redistribution, it updates bucket flags to mark the split as complete and optionally performs immediate cleanup of deleted tuples from the old bucket.

## Parameters / Member Variables
- `rel`: The hash index relation being split
- `metabuf`: Buffer containing metadata page (pinned, no lock required)
- `obucket`: Old bucket number being split
- `nbucket`: New bucket number receiving redistributed tuples
- `obuf`: Buffer for old bucket's primary page (cleanup lock required)
- `nbuf`: Buffer for new bucket's primary page (write lock, will be released)
- `htab`: Hash table of TIDs for incomplete split recovery (NULL for complete redistribution)
- `maxbucket`: Maximum bucket number for hash calculation
- `highmask`: High mask for hash-to-bucket mapping
- `lowmask`: Low mask for hash-to-bucket mapping

## Dependencies
- Functions called/Symbols referenced:
  - [PredicateLockPageSplit](../P/PredicateLockPageSplit.md)
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md)
  - ItemIdIsDead
  - [PageGetItem](../P/PageGetItem.md)
  - [hash_search](hash_search.md)
  - [_hash_hashkey2bucket](_hash_hashkey2bucket.md)
  - [_hash_get_indextuple_hashkey](_hash_get_indextuple_hashkey.md)
  - [CopyIndexTuple](../C/CopyIndexTuple.md)
  - IndexTupleSize
  - [PageGetFreeSpaceForMultipleTuples](../P/PageGetFreeSpaceForMultipleTuples.md)
  - [_hash_pgaddmultitup](_hash_pgaddmultitup.md)
  - [_hash_addovflpage](_hash_addovflpage.md)
  - [_hash_getbuf](_hash_getbuf.md)
  - [hashbucketcleanup](hashbucketcleanup.md)
  - Various WAL logging functions
- Called from (representative examples):
  - [_hash_expandtable](_hash_expandtable.md)
  - [_hash_finish_split](_hash_finish_split.md)

## Notes and Other Information
- Requires cleanup locks on both old and new buckets to prevent concurrent access
- Implements batch tuple insertion for efficiency and reduced WAL overhead
- Marks moved tuples with INDEX_MOVED_BY_SPLIT_MASK to support concurrent scans
- Supports recovery from incomplete splits via selective tuple skipping using htab
- Handles overflow page allocation automatically when new bucket fills up
- Implements proper locking order (old bucket first, then new bucket) to avoid deadlocks
- Performs immediate cleanup of old bucket if possible to reduce bloat
- Uses critical sections around shared buffer modifications for crash safety
- Copies predicate locks to maintain serializable isolation level correctness