# hash_xlog_split_allocate_page

## Location
[src/backend/access/hash/hash_xlog.c:311-427](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hash_xlog.c#L311-L427)

## Overview
Replays the page allocation phase of a hash index bucket split operation during WAL recovery, setting up the old and new bucket pages and updating metapage structures.

## Definition


## Detailed Description
This function handles WAL replay for the allocation phase of hash index bucket splitting. Hash index bucket splitting is a complex operation that occurs when buckets become full and need to be divided. This function specifically handles the page allocation and initial setup phase, preparing both the old bucket page (which will have some tuples redistributed) and the new bucket page (which will receive redistributed tuples).

The function operates on three buffers: it updates the old bucket page's special space to set appropriate flags and establish linkage to the new bucket; it initializes a new bucket page with proper bucket number and flags; and it updates the metapage to reflect the new maximum bucket number and potentially update hash masks and overflow point information. The function uses cleanup locks on both bucket pages to maintain consistency with normal operation patterns.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record with split allocation data including old_bucket_flag, new_bucket, new_bucket_flag, and optional mask/splitpoint update flags

## Dependencies
- Functions called/Symbols referenced:
  - [xl_hash_split_allocate_page](../x/xl_hash_split_allocate_page.md) (WAL record structure)
  - XLogRecGetData (extracts record data)
  - XLogReadBufferForRedoExtended (reads buffer with extended options)
  - XLogReadBufferForRedo (reads buffer for redo)
  - [XLogRecGetBlockData](../X/XLogRecGetBlockData.md) (gets block data from record)
  - [_hash_initbuf](_hash_initbuf.md) (initializes hash page buffer)
  - HashPageGetOpaque (gets page opaque data)
  - HashPageGetMeta (gets metapage metadata)
  - RBM_NORMAL (normal buffer mode)
  - RBM_ZERO_AND_CLEANUP_LOCK (zero and cleanup lock mode)
  - BLK_NEEDS_REDO (indicates block needs redo)
  - BLK_RESTORED (indicates block was restored)
  - XLH_SPLIT_META_UPDATE_MASKS (flag for mask updates)
  - XLH_SPLIT_META_UPDATE_SPLITPOINT (flag for splitpoint updates)
- Called from:
  - [hash_redo](hash_redo.md) (main hash WAL replay function)

## Notes and Other Information
- This is a static function used only within the hash WAL recovery subsystem
- The function is part of a multi-phase bucket splitting operation and handles only the allocation phase
- Cleanup locks are taken on both old and new buckets to maintain consistency with normal operation
- The old bucket page's special space is updated even when restored from a full page image since special space isn't included
- Metapage updates are conditional based on flags in the WAL record, allowing for selective updates of hash masks and overflow points
- The function handles complex metapage field updates including hashm_maxbucket, hashm_lowmask, hashm_highmask, hashm_spares, and hashm_ovflpoint
- Buffer release follows a specific order to maintain consistency with normal operation patterns