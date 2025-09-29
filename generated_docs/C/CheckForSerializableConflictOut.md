# CheckForSerializableConflictOut

## Location
[src/backend/storage/lmgr/predicate.c:4013-4155](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L4013-L4155)

## Overview
Detects and handles read-write conflicts in serializable snapshot isolation by checking if a read operation conflicts with a concurrent transaction's write operation.

## Definition
void CheckForSerializableConflictOut(Relation relation, TransactionId xid, Snapshot snapshot)

## Detailed Description
This function is the core conflict detection mechanism in PostgreSQL's serializable snapshot isolation implementation. It is called when a table access method reads a tuple that has been modified by another transaction, and determines whether this creates a serialization conflict.

The function performs several layers of analysis:

1. **Initial Validation**: Checks if serialization is needed and if the current transaction is already doomed
2. **Transaction Lookup**: Searches for the conflicting transaction in active serializable transactions or SLRU storage for old committed transactions  
3. **Summary Conflict Handling**: Handles conflicts with old transactions that have been summarized to save memory
4. **Concurrency Testing**: Uses XidIsConcurrent to determine if the transactions actually overlap
5. **Conflict Recording**: If a true conflict exists, records it using FlagRWConflict

The function implements several optimizations and special cases:
- Read-only transactions can often avoid conflicts if the writing transaction committed first
- Transactions that don't overlap (based on snapshots) don't conflict
- Already existing conflicts are not duplicated
- Dangerous structures (potential serialization anomalies) trigger immediate transaction abort

## Parameters / Member Variables
- : The relation being accessed during the read operation
- : Transaction ID of the transaction that wrote the data being read
- : Snapshot used for the read operation

## Dependencies
- Functions called/Symbols referenced:
  - [SerializationNeededForRead](../S/SerializationNeededForRead.md)
  - SxactIsDoomed
  - TransactionIdEquals
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md)
  - [LWLockAcquire](../L/LWLockAcquire.md)
  - [LWLockRelease](../L/LWLockRelease.md)
  - [hash_search](../h/hash_search.md)
  - [SerialGetMinConflictCommitSeqNo](../S/SerialGetMinConflictCommitSeqNo.md)
  - SxactIsReadOnly
  - SxactHasSummaryConflictIn
  - SxactHasSummaryConflictOut
  - SxactIsCommitted
  - SxactIsPrepared
  - SxactHasConflictOut
  - [XidIsConcurrent](../X/XidIsConcurrent.md)
  - [RWConflictExists](../R/RWConflictExists.md)
  - [FlagRWConflict](../F/FlagRWConflict.md)
- Called from:
  - [HeapCheckForSerializableConflictOut](../H/HeapCheckForSerializableConflictOut.md)
  - SerializableXactHandle (via include)

## Notes and Other Information
- This function is performance-critical as it's called for many read operations in serializable isolation level
- Uses SerializableXactHashLock to coordinate access to shared serializable transaction state
- Handles both active transactions and old committed transactions stored in SLRU
- The error messages include internal reason codes to help with debugging serialization failures
- Implements the theoretical framework for detecting dangerous structures in serializable snapshot isolation
- Can trigger immediate transaction abort when dangerous patterns are detected (pivot detection)
- Located at src/backend/storage/lmgr/predicate.c:4013

## Simplified Source

```c
void CheckForSerializableConflictOut(Relation relation, TransactionId xid, Snapshot snapshot)
{
    SERIALIZABLEXIDTAG sxidtag;
    SERIALIZABLEXID *sxid;
    SERIALIZABLEXACT *sxact;

    // Step 1: Check if serialization is needed
    if (!SerializationNeededForRead(relation, snapshot))
        return;

    // Step 2: Check if our transaction is already doomed
    if (SxactIsDoomed(MySerializableXact)) {
        ereport(ERROR, "serialization failure - transaction canceled as pivot");
    }

    // Step 3: Skip self-conflicts
    if (TransactionIdEquals(xid, GetTopTransactionIdIfAny()))
        return;

    // Step 4: Find the conflicting transaction
    sxidtag.xid = xid;
    LWLockAcquire(SerializableXactHashLock, LW_EXCLUSIVE);
    sxid = hash_search(SerializableXidHash, &sxidtag, HASH_FIND, NULL);

    if (!sxid) {
        // Check old committed transactions in SLRU
        SerCommitSeqNo conflictCommitSeqNo = SerialGetMinConflictCommitSeqNo(xid);
        if (conflictCommitSeqNo != 0) {
            // Handle conflicts with old transactions
            if (should_abort_for_old_conflict(conflictCommitSeqNo)) {
                ereport(ERROR, "serialization failure - conflict with old transaction");
            }
            MySerializableXact->flags |= SXACT_FLAG_SUMMARY_CONFLICT_OUT;
        }
        LWLockRelease(SerializableXactHashLock);
        return;
    }

    // Step 5: Get the serializable transaction
    sxact = sxid->myXact;
    if (sxact == MySerializableXact || SxactIsDoomed(sxact)) {
        LWLockRelease(SerializableXactHashLock);
        return;
    }

    // Step 6: Handle summary conflicts
    if (SxactHasSummaryConflictOut(sxact)) {
        if (!SxactIsPrepared(sxact)) {
            sxact->flags |= SXACT_FLAG_DOOMED;
        } else {
            LWLockRelease(SerializableXactHashLock);
            ereport(ERROR, "serialization failure - conflict with old pivot");
        }
        LWLockRelease(SerializableXactHashLock);
        return;
    }

    // Step 7: Read-only optimization
    if (SxactIsReadOnly(MySerializableXact) &&
        SxactIsCommitted(sxact) &&
        can_read_only_avoid_conflict(sxact)) {
        LWLockRelease(SerializableXactHashLock);
        return;
    }

    // Step 8: Check transaction concurrency
    if (!XidIsConcurrent(xid)) {
        LWLockRelease(SerializableXactHashLock);
        return;
    }

    // Step 9: Avoid duplicate conflicts
    if (RWConflictExists(MySerializableXact, sxact)) {
        LWLockRelease(SerializableXactHashLock);
        return;
    }

    // Step 10: Record the conflict
    FlagRWConflict(MySerializableXact, sxact);
    LWLockRelease(SerializableXactHashLock);
}
```