# DropRelationBuffers

## Location
src/backend/storage/buffer/bufmgr.c: 4021 - 4143

## Overview
DropRelationBuffers removes all pages of specified relation forks from the buffer pool that have block numbers greater than or equal to a given threshold, used primarily during file deletion or truncation operations.

## Definition
```c
void DropRelationBuffers(SMgrRelation smgr_reln, ForkNumber *forkNum, int nforks, BlockNumber *firstDelBlock)
```

## Detailed Description
DropRelationBuffers is a critical buffer management function that forcibly removes pages from the buffer pool without writing dirty pages to disk. It supports two optimization strategies: for small operations (below BUF_DROP_FULL_SCAN_THRESHOLD), it uses targeted buffer lookup via FindAndDropRelationBuffers; for larger operations, it performs a full buffer pool scan. The function handles both shared and local (temporary) relations appropriately, delegating local relation cleanup to DropRelationLocalBuffers. This operation is non-rollback-able and requires extreme caution as dirty pages are discarded without being written.

## Parameters / Member Variables
- `smgr_reln`: Storage manager relation containing the relation metadata and file locator information
- `forkNum`: Array of fork numbers (main, FSM, VM, etc.) to process
- `nforks`: Number of forks in the forkNum array
- `firstDelBlock`: Array of first block numbers to delete for each corresponding fork (blocks >= this value are removed)

## Dependencies
- Functions called/Symbols referenced:
  - RelFileLocatorBackendIsTemp
  - DropRelationLocalBuffers
  - smgrnblocks_cached
  - BlockNumberIsValid
  - [FindAndDropRelationBuffers](../F/FindAndDropRelationBuffers.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [BufTagMatchesRelFileLocator](../B/BufTagMatchesRelFileLocator.md)
  - LockBufHdr
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - BUF_DROP_FULL_SCAN_THRESHOLD (constant)
- Called from (representative examples):
  - smgrtruncate2

## Notes and Other Information
- **NON-ROLLBACK-ABLE**: Dirty pages are dropped without being written to disk
- Requires AccessExclusiveLock on the relation to ensure no concurrent page loading
- Uses cached relation sizes during recovery for optimization decisions
- Implements two-phase optimization: targeted lookup for small operations, full scan for large operations
- Critical for file truncation and deletion operations where affected data will be removed anyway
- Must be used only when higher-level code ensures no data loss will occur