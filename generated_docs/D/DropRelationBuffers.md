# DropRelationBuffers

## Location
[src/backend/storage/buffer/bufmgr.c:4021-4143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4021-L4143)

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
  - [DropRelationLocalBuffers](DropRelationLocalBuffers.md)
  - [smgrnblocks_cached](../s/smgrnblocks_cached.md)
  - BlockNumberIsValid
  - [FindAndDropRelationBuffers](../F/FindAndDropRelationBuffers.md)
  - [GetBufferDescriptor](../G/GetBufferDescriptor.md)
  - [BufTagMatchesRelFileLocator](../B/BufTagMatchesRelFileLocator.md)
  - [LockBufHdr](../L/LockBufHdr.md)
  - [BufTagGetForkNum](../B/BufTagGetForkNum.md)
  - [InvalidateBuffer](../I/InvalidateBuffer.md)
  - [UnlockBufHdr](../U/UnlockBufHdr.md)
  - BUF_DROP_FULL_SCAN_THRESHOLD (constant)
- Called from (representative examples):
  - [smgrtruncate2](../s/smgrtruncate2.md)

## Notes and Other Information
- **NON-ROLLBACK-ABLE**: Dirty pages are dropped without being written to disk
- Requires AccessExclusiveLock on the relation to ensure no concurrent page loading
- Uses cached relation sizes during recovery for optimization decisions
- Implements two-phase optimization: targeted lookup for small operations, full scan for large operations
- Critical for file truncation and deletion operations where affected data will be removed anyway
- Must be used only when higher-level code ensures no data loss will occur

## Simplified Source

```c
void DropRelationBuffers(SMgrRelation smgr_reln, ForkNumber *forkNum,
                        int nforks, BlockNumber *firstDelBlock)
{
    RelFileLocatorBackend rlocator = smgr_reln->smgr_rlocator;
    BlockNumber nForkBlock[MAX_FORKNUM];
    uint64 nBlocksToInvalidate = 0;

    // Handle temporary relations via local buffer manager
    if (RelFileLocatorBackendIsTemp(rlocator)) {
        if (rlocator.backend == MyProcNumber) {
            for (int j = 0; j < nforks; j++)
                DropRelationLocalBuffers(rlocator.locator, forkNum[j],
                                       firstDelBlock[j]);
        }
        return;
    }

    // Calculate total blocks to invalidate for optimization decision
    for (int i = 0; i < nforks; i++) {
        nForkBlock[i] = smgrnblocks_cached(smgr_reln, forkNum[i]);
        if (nForkBlock[i] == InvalidBlockNumber) {
            nBlocksToInvalidate = InvalidBlockNumber;
            break;
        }
        nBlocksToInvalidate += (nForkBlock[i] - firstDelBlock[i]);
    }

    // Use targeted approach for small operations
    if (BlockNumberIsValid(nBlocksToInvalidate) &&
        nBlocksToInvalidate < BUF_DROP_FULL_SCAN_THRESHOLD) {
        for (int j = 0; j < nforks; j++)
            FindAndDropRelationBuffers(rlocator.locator, forkNum[j],
                                     nForkBlock[j], firstDelBlock[j]);
        return;
    }

    // Full buffer pool scan for large operations
    for (int i = 0; i < NBuffers; i++) {
        BufferDesc *bufHdr = GetBufferDescriptor(i);

        // Quick check before locking
        if (!BufTagMatchesRelFileLocator(&bufHdr->tag, &rlocator.locator))
            continue;

        uint32 buf_state = LockBufHdr(bufHdr);

        // Check if buffer matches any fork and should be dropped
        for (int j = 0; j < nforks; j++) {
            if (BufTagMatchesRelFileLocator(&bufHdr->tag, &rlocator.locator) &&
                BufTagGetForkNum(&bufHdr->tag) == forkNum[j] &&
                bufHdr->tag.blockNum >= firstDelBlock[j]) {
                InvalidateBuffer(bufHdr);  // releases lock
                break;
            }
        }
        if (j >= nforks)
            UnlockBufHdr(bufHdr, buf_state);
    }
}
```