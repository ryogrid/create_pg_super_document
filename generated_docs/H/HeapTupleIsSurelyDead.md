# HeapTupleIsSurelyDead

## Location
src/backend/access/heap/heapam_visibility.c: 1465 - 1519

## Overview
HeapTupleIsSurelyDead provides a fast way to determine whether a tuple is surely dead to all onlookers by examining hint bits without consulting the process array or commit log.

## Definition
bool HeapTupleIsSurelyDead(HeapTuple htup, GlobalVisState *vistest)

## Detailed Description
This function serves as a cheaper alternative to HeapTupleSatisfiesVacuum when a tuple has recently been tested by another visibility routine (like HeapTupleSatisfiesMVCC). It assumes that if no hint bits are set, the associated transaction is still running, making it faster than full visibility checks since it avoids consulting the process array or CLOG.

The function follows a systematic approach to determine tuple death:
1. First checks if the inserting transaction (xmin) is marked as committed or invalid
2. If committed, examines the deleting transaction (xmax) status
3. Handles special cases like locks, MultiXacts, and uncommitted deleters
4. For committed deleters, uses the visibility test to determine if the XID is old enough to be considered removable

The function is designed to be conservative - it's acceptable to return false when in doubt, but must only return true when the tuple is definitely removable.

## Parameters / Member Variables
- `htup`: The heap tuple to check for death status
- `vistest`: Global visibility state used to determine if transaction IDs are old enough to be removable

## Dependencies
- Functions called/Symbols referenced:
  - ItemPointerIsValid
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid
  - HEAP_XMAX_IS_LOCKED_ONLY
  - GlobalVisTestIsRemovableXid
  - HeapTupleHeaderGetRawXmax
  - HEAP_XMAX_INVALID (macro)
  - HEAP_XMAX_IS_MULTI (macro)
  - HEAP_XMAX_COMMITTED (macro)
- Called from (representative examples):
  - heap_hot_search_buffer
  - HeapScanIsValid

## Notes and Other Information
- This function is optimized for performance by avoiding expensive visibility checks when hint bits are available
- It assumes that hint bits have been recently set by previous visibility checks
- The function includes assertions to validate that the tuple has a valid ItemPointer and table OID
- It returns false for MultiXacts since determining their status requires checking pg_multixact
- The function is conservative in its approach - preferring false negatives over false positives to ensure data integrity