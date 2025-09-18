# MarkBufferDirtyHint

## Location
src/backend/storage/buffer/bufmgr.c: 4961 - 5103

## Overview
MarkBufferDirtyHint marks a buffer dirty for non-critical hint bit updates, with special handling for WAL logging and race conditions.

## Definition


## Detailed Description
This function is designed for marking buffers dirty when making non-critical changes like hint bit updates. Unlike MarkBufferDirty, it has several key differences: it may need to write WAL records for checksum protection when the caller doesn't write WAL, it can work with share locks instead of requiring exclusive locks, and it doesn't guarantee the buffer will always be marked dirty due to potential race conditions. The function implements sophisticated logic to handle torn page protection, checkpoint coordination, and vacuum cost accounting while being optimized for performance in high-frequency hint bit update scenarios.

## Parameters / Member Variables
- : The Buffer identifier for the buffer to mark dirty
- : Boolean indicating if this is a standard relation buffer (affects WAL logging decisions)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetPage](../B/BufferGetPage.md), BufferIsValid, BufferIsLocal
  - MarkLocalBufferDirty, GetBufferDescriptor
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