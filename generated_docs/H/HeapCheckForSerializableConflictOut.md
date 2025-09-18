# HeapCheckForSerializableConflictOut

## Location
src/backend/access/heap/heapam.c: 10519 - 10600

## Overview
Detects potential serializable isolation conflicts when reading heap tuples by checking for read-write conflicts with concurrent transactions.

## Definition
```c
void HeapCheckForSerializableConflictOut(bool visible, Relation relation,
                                       HeapTuple tuple, Buffer buffer,
                                       Snapshot snapshot)
```

## Detailed Description
This function implements a critical component of PostgreSQL's Serializable Snapshot Isolation (SSI) by detecting potential read-write conflicts during tuple reads. It examines whether a tuple being read was written by a concurrent transaction that could create a serialization conflict, either by inserting a tuple that's not visible to the current transaction, or by deleting a tuple that is visible.

The function uses HeapTupleSatisfiesVacuum to determine the tuple's state and extracts the appropriate transaction ID (either xmin for insertions or update xid for deletions). It then resolves to the top-level transaction ID and calls CheckForSerializableConflictOut to register the potential conflict for later analysis by the SSI system.

## Parameters / Member Variables
- `visible`: Boolean indicating whether the tuple is visible to the current transaction's snapshot
- `relation`: The relation being read from
- `tuple`: The heap tuple being examined for conflicts  
- `buffer`: Buffer containing the page with the tuple (must be locked)
- `snapshot`: The snapshot being used for the read operation

## Dependencies
- Functions called/Symbols referenced:
  - CheckForSerializableConflictOutNeeded (checks if conflict detection is needed)
  - HeapTupleSatisfiesVacuum (determines tuple visibility state)
  - HeapTupleHeaderGetXmin, HeapTupleHeaderGetUpdateXid (extract transaction IDs)
  - TransactionIdPrecedes, TransactionIdEquals, TransactionIdFollowsOrEquals (transaction ID comparisons)
  - GetTopTransactionIdIfAny (gets current transaction ID)
  - SubTransGetTopmostTransaction (resolves subtransaction to top-level)
  - CheckForSerializableConflictOut (registers the conflict)
- Called from (representative examples):
  - heapgettup (during sequential scans)
  - heap_fetch (during index lookups)
  - heap_hot_search_buffer (during HOT chain following)
  - heapam_scan_bitmap_next_block (during bitmap scans)

## Notes and Other Information
- Essential for maintaining SERIALIZABLE isolation level correctness
- Must be called with at least shared lock on buffer as it may set hint bits
- Handles different conflict scenarios based on tuple state (LIVE, RECENTLY_DEAD, etc.)
- Only processes conflicts with transactions that started after TransactionXmin
- Part of PostgreSQL's sophisticated concurrency control system
- No known reason to call from index access methods, only heap access methods
- Uses top-level transaction IDs to properly handle subtransactions in conflict detection