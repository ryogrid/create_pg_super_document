# HeapTupleSatisfiesToast

## Location
[src/backend/access/heap/heapam_visibility.c:362-457](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_visibility.c#L362-L457)

## Overview
Specialized visibility function for TOAST tuples that performs simplified checks focused on VACUUM moving conditions, designed for PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system.

## Definition

```c
static bool
HeapTupleSatisfiesToast(HeapTuple htup, Snapshot snapshot,
						Buffer buffer)
```
## Detailed Description
HeapTupleSatisfiesToast implements specialized visibility semantics for TOAST tables. TOAST is PostgreSQL's mechanism for storing large attribute values separately from the main table row. This visibility function is simplified compared to regular tuple visibility functions because TOAST tables have different access patterns and requirements.

The key design principle is that if you can see a main table row containing a TOAST reference, you should be able to see the corresponding TOAST value. Therefore, this function primarily focuses on:
- Checking for VACUUM-related moving conditions (HEAP_MOVED_OFF/HEAP_MOVED_IN from pre-9.0 upgrades)
- Handling speculative insertion cancellations
- Basic transaction validity checks for Xmin

The function deliberately avoids full MVCC time qualification checks since TOAST access is inherently tied to the visibility of the referencing main table tuple. However, it includes essential safety checks to handle cases where TOAST table vacuuming fails partway through.

## Parameters / Member Variables
- `htup`: The TOAST tuple to check for visibility
- `snapshot`: Snapshot context (used with SnapshotToast)
- `buffer`: Buffer containing the TOAST tuple, used for hint bit updates

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderXminCommitted
  - HeapTupleHeaderXminInvalid
  - HeapTupleHeaderGetXvac
  - HeapTupleHeaderGetXmin
  - [TransactionIdIsCurrentTransactionId](../T/TransactionIdIsCurrentTransactionId.md)
  - TransactionIdIsInProgress
  - [TransactionIdDidCommit](../T/TransactionIdDidCommit.md)
  - [SetHintBits](../S/SetHintBits.md)
  - [ItemPointerIsValid](../I/ItemPointerIsValid.md)
- Called from (representative examples):
  - [HeapTupleSatisfiesVisibility](HeapTupleSatisfiesVisibility.md)

## Notes and Other Information
- This is a simplified visibility function specifically designed for TOAST tables
- TOAST tuples cannot be updated directly (only through the main table), which simplifies the visibility logic
- Includes legacy support for pre-9.0 VACUUM FULL operations via HEAP_MOVED_OFF/HEAP_MOVED_IN handling
- Handles speculative insertion scenarios where tuples may be canceled by super-deletion
- Part of PostgreSQL's TOAST system architecture, which stores large values separately from main table rows
- Uses hint bits for performance optimization while maintaining the simplified checking logic