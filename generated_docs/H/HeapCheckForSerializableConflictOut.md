# HeapCheckForSerializableConflictOut

## Location
[src/backend/access/heap/heapam.c:10519-10600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L10519-L10600)

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
  - [CheckForSerializableConflictOutNeeded](../C/CheckForSerializableConflictOutNeeded.md) (checks if conflict detection is needed)
  - [HeapTupleSatisfiesVacuum](HeapTupleSatisfiesVacuum.md) (determines tuple visibility state)
  - HeapTupleHeaderGetXmin, HeapTupleHeaderGetUpdateXid (extract transaction IDs)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md), TransactionIdEquals, TransactionIdFollowsOrEquals (transaction ID comparisons)
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md) (gets current transaction ID)
  - [SubTransGetTopmostTransaction](../S/SubTransGetTopmostTransaction.md) (resolves subtransaction to top-level)
  - [CheckForSerializableConflictOut](../C/CheckForSerializableConflictOut.md) (registers the conflict)
- Called from (representative examples):
  - [heapgettup](../h/heapgettup.md) (during sequential scans)
  - [heap_fetch](../h/heap_fetch.md) (during index lookups)
  - [heap_hot_search_buffer](../h/heap_hot_search_buffer.md) (during HOT chain following)
  - [heapam_scan_bitmap_next_block](../h/heapam_scan_bitmap_next_block.md) (during bitmap scans)

## Notes and Other Information
- Essential for maintaining SERIALIZABLE isolation level correctness
- Must be called with at least shared lock on buffer as it may set hint bits
- Handles different conflict scenarios based on tuple state (LIVE, RECENTLY_DEAD, etc.)
- Only processes conflicts with transactions that started after TransactionXmin
- Part of PostgreSQL's sophisticated concurrency control system
- No known reason to call from index access methods, only heap access methods
- Uses top-level transaction IDs to properly handle subtransactions in conflict detection

## Simplified Source

```c
void HeapCheckForSerializableConflictOut(bool visible, Relation relation,
                                        HeapTuple tuple, Buffer buffer,
                                        Snapshot snapshot)
{
    TransactionId xid;
    HTSV_Result htsvResult;

    // Early exit if conflict checking not needed
    if (!CheckForSerializableConflictOutNeeded(relation, snapshot))
        return;

    // Determine tuple state and extract relevant transaction ID
    htsvResult = HeapTupleSatisfiesVacuum(tuple, TransactionXmin, buffer);

    switch (htsvResult) {
        case HEAPTUPLE_LIVE:
            if (visible)
                return;  // No conflict for visible live tuples
            xid = HeapTupleHeaderGetXmin(tuple->t_data);
            break;

        case HEAPTUPLE_RECENTLY_DEAD:
        case HEAPTUPLE_DELETE_IN_PROGRESS:
            if (visible)
                xid = HeapTupleHeaderGetUpdateXid(tuple->t_data);
            else
                xid = HeapTupleHeaderGetXmin(tuple->t_data);

            if (TransactionIdPrecedes(xid, TransactionXmin)) {
                Assert(!visible);
                return;  // Too old to be a conflict
            }
            break;

        case HEAPTUPLE_INSERT_IN_PROGRESS:
            xid = HeapTupleHeaderGetXmin(tuple->t_data);
            break;

        case HEAPTUPLE_DEAD:
            Assert(!visible);
            return;

        default:
            elog(ERROR, "unrecognized return value from HeapTupleSatisfiesVacuum: %u",
                 htsvResult);
            xid = InvalidTransactionId;  // Silence compiler warning
    }

    Assert(TransactionIdIsValid(xid));
    Assert(TransactionIdFollowsOrEquals(xid, TransactionXmin));

    // Skip if it's our own transaction
    if (TransactionIdEquals(xid, GetTopTransactionIdIfAny()))
        return;

    // Get top-level transaction and check for conflict
    xid = SubTransGetTopmostTransaction(xid);
    if (TransactionIdPrecedes(xid, TransactionXmin))
        return;

    CheckForSerializableConflictOut(relation, xid, snapshot);
}
```