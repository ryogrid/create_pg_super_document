# HeapTupleSatisfiesHistoricMVCC

## Location
src/backend/access/heap/heapam_visibility.c: 1587 - 1766

## Overview
HeapTupleSatisfiesHistoricMVCC implements historic MVCC visibility semantics for catalog tables, determining tuple visibility for time-travel queries during logical decoding.

## Definition
static bool HeapTupleSatisfiesHistoricMVCC(HeapTuple htup, Snapshot snapshot, Buffer buffer)

## Detailed Description
This function provides historic MVCC visibility checking specifically designed for catalog tables during logical decoding operations. It follows the same general semantics as HeapTupleSatisfiesMVCC but operates within a historic context, allowing time-travel queries to see the database state as it existed at a particular point in time.

The function performs a comprehensive visibility check by examining both the inserting transaction (xmin) and deleting transaction (xmax). For transactions within the snapshot's transaction arrays, it must resolve combo CIDs to determine actual command IDs for proper visibility determination.

Key features include:
- Only supports catalog tables (not user tables with HEAP_MOVED_(IN|OFF))
- Does not set hint bits to avoid complications during time travel
- Handles combo CIDs through ResolveCminCmaxDuringDecoding
- Supports both regular and streaming logical decoding scenarios
- Manages MultiXact cases for shared locks and updates

## Parameters / Member Variables
- `htup`: The heap tuple to check for historic visibility
- `snapshot`: Historic snapshot containing transaction arrays and visibility boundaries
- `buffer`: Buffer containing the tuple

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderGetXmin
  - HeapTupleHeaderGetRawXmax
  - HeapTupleHeaderXminInvalid
  - HeapTupleHeaderXminCommitted
  - TransactionIdInArray
  - TransactionIdDidCommit
  - TransactionIdPrecedes
  - TransactionIdFollowsOrEquals
  - ResolveCminCmaxDuringDecoding
  - HistoricSnapshotGetTupleCids
  - HeapTupleGetUpdateXid
  - HeapTupleHeaderGetRawCommandId
  - ItemPointerIsValid
  - HEAP_XMAX_INVALID, HEAP_XMAX_IS_LOCKED_ONLY, HEAP_XMAX_IS_MULTI, HEAP_XMAX_COMMITTED (macros)
- Called from (representative examples):
  - HeapTupleSatisfiesVisibility

## Notes and Other Information
- This is a static function, only accessible within the heapam_visibility.c file
- Specifically designed for catalog table access during logical decoding operations
- Does not support HEAP_MOVED_(IN|OFF) since catalog pages aren't created in older versions
- Avoids setting hint bits due to the complexity of time-travel semantics
- Handles unresolved combo CIDs by treating them as future commands for in-progress transaction decoding
- Contains extensive comments explaining combo CID resolution behavior for different decoding scenarios
- The function includes assertions to validate tuple structure and transaction state consistency