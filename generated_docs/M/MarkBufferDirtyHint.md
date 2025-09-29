# MarkBufferDirtyHint

## Location
[src/backend/storage/buffer/bufmgr.c:4961-5103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L4961-L5103)

## Overview
MarkBufferDirtyHint marks a buffer dirty for non-critical hint bit updates, with special handling for WAL logging and race conditions.

## Definition

```c
void
MarkBufferDirtyHint(Buffer buffer, bool buffer_std)
```
## Detailed Description
This function is designed for marking buffers dirty when making non-critical changes like hint bit updates. Unlike MarkBufferDirty, it has several key differences: it may need to write WAL records for checksum protection when the caller doesn't write WAL, it can work with share locks instead of requiring exclusive locks, and it doesn't guarantee the buffer will always be marked dirty due to potential race conditions. The function implements sophisticated logic to handle torn page protection, checkpoint coordination, and vacuum cost accounting while being optimized for performance in high-frequency hint bit update scenarios.

## Parameters / Member Variables
- : The Buffer identifier for the buffer to mark dirty
- : Boolean indicating if this is a standard relation buffer (affects WAL logging decisions)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md), BufferIsValid, BufferIsLocal
  - [MarkLocalBufferDirty](MarkLocalBufferDirty.md), GetBufferDescriptor
  - [GetPrivateRefCount](../G/GetPrivateRefCount.md), LWLockHeldByMe, BufferDescriptorGetContentLock
  - [pg_atomic_read_u32](../p/pg_atomic_read_u32.md), XLogHintBitIsNeeded
  - [RecoveryInProgress](../R/RecoveryInProgress.md), RelFileLocatorSkippingWAL, BufTagGetRelFileLocator  
  - [XLogSaveBufferForHint](../X/XLogSaveBufferForHint.md), LockBufHdr, UnlockBufHdr
  - [PageSetLSN](../P/PageSetLSN.md), XLogRecPtrIsInvalid
  - Various buffer state flags (BM_DIRTY, BM_JUST_DIRTIED, BM_PERMANENT)
- Called from (representative examples):
  - [SetHintBits](../S/SetHintBits.md) (heap visibility operations)
  - [_bt_killitems](../b/_bt_killitems.md) (B-tree index cleanup)
  - [gistkillitems](../g/gistkillitems.md) (GiST index cleanup) 
  - [heap_page_prune_and_freeze](../h/heap_page_prune_and_freeze.md) (heap maintenance)
  - Free space map operations (fsm_set_and_search, fsm_vacuum_page)

## Notes and Other Information
- Optimized for high-frequency operations with early exit if buffer already dirty
- Handles both local and shared buffers with different code paths
- Implements checkpoint delay mechanism to prevent race conditions with WAL logging
- May write full-page images to WAL when checksums are enabled and hint bit protection is needed
- Does not guarantee buffer will be marked dirty due to potential race conditions
- Integrates with vacuum cost accounting system
- Critical for hint bit updates in heap visibility checks and index maintenance operations

## Simplified Source

```c
void
MarkBufferDirtyHint(Buffer buffer, bool buffer_std)
{
    BufferDesc *buf_hdr;
    Page page = BufferGetPage(buffer);

    if (!BufferIsValid(buffer))
        elog(ERROR, "bad buffer ID: %d", buffer);

    // Handle local buffers separately
    if (BufferIsLocal(buffer))
    {
        MarkLocalBufferDirty(buffer);
        return;
    }

    buf_hdr = GetBufferDescriptor(buffer - 1);

    // Quick exit if already dirty (unlocked check for performance)
    if ((pg_atomic_read_u32(&buf_hdr->state) & (BM_DIRTY | BM_JUST_DIRTIED)) !=
        (BM_DIRTY | BM_JUST_DIRTIED))
    {
        XLogRecPtr lsn = InvalidXLogRecPtr;
        bool dirtied = false;
        bool delay_chkpt_flags = false;
        uint32 buf_state;

        // WAL logging for hint bit protection (torn page protection)
        if (XLogHintBitIsNeeded() &&
            (pg_atomic_read_u32(&buf_hdr->state) & BM_PERMANENT))
        {
            // Skip if in recovery or file skips WAL
            if (RecoveryInProgress() ||
                RelFileLocatorSkippingWAL(BufTagGetRelFileLocator(&buf_hdr->tag)))
                return;

            // Delay checkpoint to prevent race conditions
            MyProc->delayChkptFlags |= DELAY_CHKPT_START;
            delay_chkpt_flags = true;
            lsn = XLogSaveBufferForHint(buffer, buffer_std);
        }

        // Acquire buffer header lock and mark dirty
        buf_state = LockBufHdr(buf_hdr);

        if (!(buf_state & BM_DIRTY))
        {
            dirtied = true;

            // Set LSN if we wrote a backup block
            if (!XLogRecPtrIsInvalid(lsn))
                PageSetLSN(page, lsn);
        }

        buf_state |= BM_DIRTY | BM_JUST_DIRTIED;
        UnlockBufHdr(buf_hdr, buf_state);

        // Clear checkpoint delay flag
        if (delay_chkpt_flags)
            MyProc->delayChkptFlags &= ~DELAY_CHKPT_START;

        // Update statistics if buffer was dirtied
        if (dirtied)
        {
            VacuumPageDirty++;
            pgBufferUsage.shared_blks_dirtied++;
            if (VacuumCostActive)
                VacuumCostBalance += VacuumCostPageDirty;
        }
    }
}
```