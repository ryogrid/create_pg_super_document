# heap_pre_freeze_checks

## Location
src/backend/access/heap/heapam.c: 7306 - 7358

## Overview
Performs expensive transaction status validation checks on tuples before executing freeze plans, ensuring that transaction IDs are in the expected committed/aborted state before freezing proceeds.

## Definition


## Detailed Description
heap_pre_freeze_checks is a validation function that performs expensive transaction status checks before freeze plans are executed. This function was separated from heap_prepare_freeze_tuple to avoid performing costly pg_xact lookups during freeze plan preparation, allowing multiple VACUUM operations to reuse the same freeze plans without repeating expensive validation.

The function validates two critical conditions:
1. **XMIN_COMMITTED checks**: Verifies that xmin transactions marked for freezing have actually committed, preventing freezing of uncommitted transactions
2. **XMAX_ABORTED checks**: Ensures that xmax transactions being frozen have not committed (they should be aborted or in-progress-during-crash)

The function deliberately avoids relying on tuple hint bits, performing direct transaction status lookups to ensure accuracy. This approach prevents data corruption that could occur if freeze plans were executed based on incorrect assumptions about transaction states.

## Parameters
- : Buffer containing the page with tuples to validate
- : Array of HeapTupleFreeze structures containing freeze plans and check flags
- : Number of tuples in the array to validate

## Dependencies
- Functions called/Symbols referenced:
  - BufferGetPage
  - PageGetItemId
  - PageGetItem
  - HeapTupleHeaderGetRawXmin
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderXminFrozen
  - TransactionIdDidCommit
  - TransactionIdIsNormal
  - HEAP_FREEZE_CHECK_XMIN_COMMITTED
  - HEAP_FREEZE_CHECK_XMAX_ABORTED
  - ERRCODE_DATA_CORRUPTED
- Called from:
  - heap_page_prune_and_freeze
  - HeapScanIsValid (via header inclusion)

## Notes and Other Information
- **Performance Optimization**: Separated from freeze preparation to avoid repeating expensive pg_xact lookups
- **Hint Bit Independence**: Deliberately avoids using tuple hint bits, performing authoritative transaction status checks
- **Error Detection**: Reports data corruption when transaction states don't match expectations
- **Crash Recovery**: Handles cases where transactions were in-progress during system crashes
- **Check Flags**: Uses HEAP_FREEZE_CHECK_XMIN_COMMITTED and HEAP_FREEZE_CHECK_XMAX_ABORTED flags to determine which validations to perform
- **Xmax Limitations**: Cannot reliably use TransactionIdDidAbort due to crash scenarios, only checks that xmax didn't commit
- **Buffer Integration**: Works with shared buffer pages, expecting caller to hold appropriate locks
- **VACUUM Integration**: Designed for use in VACUUM operations where freeze plans may be reused across multiple operations