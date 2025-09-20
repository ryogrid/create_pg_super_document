# _hash_squeezebucket

## Location
[src/backend/access/hash/hashovfl.c:842-1125](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashovfl.c#L842-L1125)

## Overview
Compacts tuples within a hash bucket chain by moving tuples from later pages to earlier pages to maximize space utilization and free unnecessary overflow pages.

## Definition

```c
void
_hash_squeezebucket(Relation rel,
					Bucket bucket,
					BlockNumber bucket_blkno,
					Buffer bucket_buf,
					BufferAccessStrategy bstrategy)
```
## Detailed Description
This function implements bucket compaction for hash indexes during VACUUM operations. It uses a two-pointer approach: a "write" pointer starting from the primary bucket page moving forward, and a "read" pointer starting from the last overflow page moving backward. The algorithm moves tuples from the read pages to fill available space in write pages, thereby eliminating empty or underutilized overflow pages.

The function maintains hashkey ordering when inserting moved tuples and uses WAL logging for crash safety. It employs lock chaining to prevent concurrent scans from seeing inconsistent bucket states during the reorganization process. All pages in the bucket chain are guaranteed to be non-empty after completion, unless the entire bucket is empty.

## Parameters / Member Variables
- : Relation (hash index) being processed
- : Bucket number being compacted  
- : Block number of the primary bucket page
- : Buffer containing the primary bucket page (must be cleanup-locked)
- : Buffer access strategy for controlling page fetches during VACUUM

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md), HashPageGetOpaque
  - BlockNumberIsValid, LockBuffer  
  - [_hash_relbuf](_hash_relbuf.md), _hash_getbuf_with_strategy
  - [PageGetMaxOffsetNumber](../P/PageGetMaxOffsetNumber.md), PageGetItem, PageGetItemId
  - ItemIdIsDead, IndexTupleSize
  - [PageGetFreeSpaceForMultipleTuples](../P/PageGetFreeSpaceForMultipleTuples.md)
  - [_hash_pgaddmultitup](_hash_pgaddmultitup.md), PageIndexMultiDelete
  - [CopyIndexTuple](../C/CopyIndexTuple.md), _hash_freeovflpage
  - WAL functions: XLogEnsureRecordSpace, XLogBeginInsert, XLogRegisterData, XLogInsert
- Types/Constants referenced:
  - HashPageOpaque, IndexTuple, OffsetNumber
  - MaxOffsetNumber, MaxIndexTuplesPerPage
  - HASH_WRITE, LH_OVERFLOW_PAGE
  - XLOG_HASH_MOVE_PAGE_CONTENTS
- Called from:
  - [hashbucketcleanup](hashbucketcleanup.md) (primary caller during VACUUM)

## Notes and Other Information
- Requires cleanup lock on primary bucket page to exclude concurrent scans
- Uses lock chaining technique to prevent scan-squeeze interference  
- Preserves hashkey ordering when moving tuples between pages
- Handles WAL logging for multi-tuple operations with XLogEnsureRecordSpace
- Supports buffer access strategy to control memory usage during VACUUM
- Empty overflow pages encountered during the process are automatically freed
- The algorithm terminates when read and write pointers meet at the same page
- Critical sections protect the tuple movement operations for crash safety