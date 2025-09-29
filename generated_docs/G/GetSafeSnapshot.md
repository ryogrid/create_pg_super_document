# GetSafeSnapshot

## Location
[src/backend/storage/lmgr/predicate.c:1548-1617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L1548-L1617)

## Overview
Obtains and registers a snapshot for READ ONLY DEFERRABLE transactions that is guaranteed to be "safe" for serializable execution without further conflict checks.

## Definition
static Snapshot GetSafeSnapshot(Snapshot origSnapshot)

## Detailed Description
This function is a specialized subroutine of GetSerializableTransactionSnapshot that handles READ ONLY DEFERRABLE transactions. It ensures that the returned snapshot is "safe" - meaning a read-only transaction can execute serializably using this snapshot without needing additional serialization checks.

The function implements a retry loop that:
1. Obtains a serializable transaction snapshot
2. If no concurrent read/write transactions exist, the snapshot is immediately safe
3. Otherwise, it waits for potentially conflicting concurrent transactions to complete
4. If marked as unsafe during the wait, it releases locks and retries with a new snapshot
5. Once a safe snapshot is obtained, it releases predicate locks since no further checks are needed

The "deferrable" aspect allows the transaction to wait (potentially indefinitely) until a safe snapshot becomes available, trading immediate execution for guaranteed serializable behavior without conflicts.

## Parameters / Member Variables
- : Pointer to static snapshot area that can be passed to GetSnapshotData

## Dependencies
- Functions called/Symbols referenced:
  - [GetSerializableTransactionSnapshotInt](GetSerializableTransactionSnapshotInt.md)
  - InvalidPid
  - InvalidSerializableXact
  - SXACT_FLAG_DEFERRABLE_WAITING
  - [dlist_is_empty](../d/dlist_is_empty.md)
  - SxactIsROUnsafe
  - [ProcWaitForSignal](../P/ProcWaitForSignal.md)
  - DEBUG2
  - ERRCODE_T_R_SERIALIZATION_FAILURE
  - [ReleasePredicateLocks](../R/ReleasePredicateLocks.md)
  - SxactIsROSafe
- Called from (representative examples):
  - [SerialControl](../S/SerialControl.md) (during setup)
  - [GetSerializableTransactionSnapshot](GetSerializableTransactionSnapshot.md) (for deferrable transactions)

## Notes and Other Information
- This is a static function local to predicate.c
- Only called for transactions that are both XactReadOnly and XactDeferrable
- Implements a retry mechanism that may wait indefinitely for safety
- Uses WAIT_EVENT_SAFE_SNAPSHOT for process waiting
- May log DEBUG2 messages when retrying due to unsafe snapshots
- Part of PostgreSQL's Serializable Snapshot Isolation (SSI) implementation
- The returned snapshot requires no further serialization conflict checking
- Located in src/backend/storage/lmgr/predicate.c:1548-1617

## Simplified Source

```c
// Simplified version of GetSafeSnapshot
static Snapshot
GetSafeSnapshot(Snapshot origSnapshot)
{
    Snapshot snapshot;

    // Ensure we're in a READ ONLY DEFERRABLE transaction
    Assert(XactReadOnly && XactDeferrable);

    while (true)
    {
        // Get a serializable transaction snapshot
        snapshot = GetSerializableTransactionSnapshotInt(origSnapshot, NULL, InvalidPid);

        // If no concurrent read/write transactions, snapshot is immediately safe
        if (MySerializableXact == InvalidSerializableXact)
            return snapshot;

        // Wait for potentially conflicting concurrent transactions to finish
        LWLockAcquire(SerializableXactHashLock, LW_EXCLUSIVE);

        MySerializableXact->flags |= SXACT_FLAG_DEFERRABLE_WAITING;

        // Wait until no unsafe conflicts remain or we're marked as unsafe
        while (!(dlist_is_empty(&MySerializableXact->possibleUnsafeConflicts) ||
                 SxactIsROUnsafe(MySerializableXact)))
        {
            LWLockRelease(SerializableXactHashLock);
            ProcWaitForSignal(WAIT_EVENT_SAFE_SNAPSHOT);
            LWLockAcquire(SerializableXactHashLock, LW_EXCLUSIVE);
        }

        MySerializableXact->flags &= ~SXACT_FLAG_DEFERRABLE_WAITING;

        // Check if snapshot is now safe
        if (!SxactIsROUnsafe(MySerializableXact))
        {
            LWLockRelease(SerializableXactHashLock);
            break; // Success - we have a safe snapshot
        }

        LWLockRelease(SerializableXactHashLock);

        // Snapshot became unsafe, retry with a new one
        ereport(DEBUG2, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg_internal("deferrable snapshot was unsafe; trying a new one")));
        ReleasePredicateLocks(false, false);
    }

    // We now have a safe snapshot - no further checks needed
    Assert(SxactIsROSafe(MySerializableXact));
    ReleasePredicateLocks(false, true);

    return snapshot;
}
```

Key simplifications made:
- Preserved all essential logic flow and error handling
- Added descriptive comments explaining each major step
- Maintained the retry loop structure which is core to the algorithm
- Kept all critical assertions and safety checks
- Preserved the waiting mechanism for concurrent transaction completion
- Maintained proper lock acquisition/release patterns
- Kept debug logging for troubleshooting unsafe snapshot retries