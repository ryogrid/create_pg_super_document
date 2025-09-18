# log_newpage_range

## Location
[src/backend/access/transam/xloginsert.c:1270-1347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L1270-L1347)

## Overview
log_newpage_range writes WAL records containing full images of a range of blocks in a relation, processing large ranges in batches to handle multiple pages efficiently.

## Definition
```c
void log_newpage_range(Relation rel, ForkNumber forknum, BlockNumber startblk, BlockNumber endblk, bool page_std)
```

## Detailed Description
This function creates Write-Ahead Log (WAL) records containing complete images of a contiguous range of pages within a relation for crash recovery purposes. It efficiently handles large ranges by processing pages in batches of up to XLR_MAX_BLOCK_ID pages per WAL record. The function acquires exclusive locks on all pages in the range and skips completely empty pages to avoid changing their LSN unnecessarily.

The function is typically used on newly-built relations where the caller holds an AccessExclusiveLock, ensuring no concurrent access. It supports the same page layout optimization as other newpage functions, allowing unused space in standard page layouts to be excluded from WAL records when page_std is true.

The function implements a sophisticated batching mechanism that reads and locks pages in groups, creates a single WAL record for each batch, and properly manages buffer lifecycle including setting LSNs and releasing locks.

## Parameters / Member Variables
- `rel`: The relation containing the pages to be logged
- `forknum`: The fork number within the relation (main, FSM, VM, etc.)
- `startblk`: The first block number in the range (inclusive)
- `endblk`: The last block number in the range (exclusive)
- `page_std`: Boolean flag indicating whether pages follow standard layout for optimization

## Dependencies
- Functions called/Symbols referenced:
  - [XLogEnsureRecordSpace](../X/XLogEnsureRecordSpace.md) (ensures sufficient WAL record space)
  - [ReadBufferExtended](../R/ReadBufferExtended.md) (reads buffers with RBM_NORMAL mode)
  - [LockBuffer](../L/LockBuffer.md) (acquires BUFFER_LOCK_EXCLUSIVE)
  - [PageIsNew](../P/PageIsNew.md) (checks if page is completely empty)
  - [XLogBeginInsert](../X/XLogBeginInsert.md) (begins WAL record construction)
  - [XLogRegisterBuffer](../X/XLogRegisterBuffer.md) (registers buffers with REGBUF_FORCE_IMAGE flags)
  - [XLogInsert](../X/XLogInsert.md) (finalizes WAL record with RM_XLOG_ID, XLOG_FPI)
  - MarkBufferDirty (marks buffers as dirty)
  - [PageSetLSN](../P/PageSetLSN.md) (sets page LSN)
  - [UnlockReleaseBuffer](../U/UnlockReleaseBuffer.md) (releases buffer locks)
- Called from (representative examples):
  - [ginbuild](../g/ginbuild.md) (GIN index building)
  - [gistbuild](../g/gistbuild.md) (GiST index building)
  - [spgbuild](../s/spgbuild.md) (SP-GiST index building)
  - [smgrDoPendingSyncs](../s/smgrDoPendingSyncs.md) (storage manager sync operations)

## Notes and Other Information
- Acquires exclusive locks on all pages in the range - caller must ensure no deadlock risk
- Typically used during relation building with AccessExclusiveLock held
- Skips completely empty pages to preserve their empty state (no LSN change)
- Processes pages in batches of XLR_MAX_BLOCK_ID to manage WAL record size
- Uses REGBUF_FORCE_IMAGE flag to ensure full page images are logged
- Critical sections protect the WAL logging and LSN setting operations
- CHECK_FOR_INTERRUPTS() allows for query cancellation during large operations
- When page_std is true, unused space between pd_lower and pd_upper is excluded from WAL records