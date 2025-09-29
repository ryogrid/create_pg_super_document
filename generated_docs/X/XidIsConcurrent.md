# XidIsConcurrent

## Location
[src/backend/storage/lmgr/predicate.c:3962-3980](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L3962-L3980)

## Overview
Determines whether a given top-level transaction ID is concurrent (overlapping) with the current transaction for serializable snapshot isolation purposes.

## Definition
static bool XidIsConcurrent(TransactionId xid)

## Detailed Description
This function tests whether a specified transaction ID represents a transaction that was running concurrently with the current transaction. It uses the current transaction's snapshot to make this determination, which is essential for detecting potential serialization conflicts in PostgreSQL's serializable snapshot isolation.

The function implements a three-step check:
1. **Historical transactions**: If the XID precedes the snapshot's xmin, it committed before the current transaction started, so it's not concurrent
2. **Future transactions**: If the XID is greater than or equal to the snapshot's xmax, it started after the current transaction's snapshot was taken, so it is concurrent
3. **In-progress transactions**: For XIDs between xmin and xmax, check if the XID appears in the snapshot's active transaction list (xip array)

This concurrency detection is crucial for the serializable snapshot isolation implementation to identify which transactions might have conflicting access patterns that could violate serializability.

## Parameters / Member Variables
- : The transaction ID to test for concurrency with the current transaction

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - TransactionIdEquals
  - [GetTopTransactionIdIfAny](../G/GetTopTransactionIdIfAny.md)
  - [GetTransactionSnapshot](../G/GetTransactionSnapshot.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdFollowsOrEquals](../T/TransactionIdFollowsOrEquals.md)
  - [pg_lfind32](../p/pg_lfind32.md)
- Called from:
  - [SerialControl](../S/SerialControl.md)
  - [CheckForSerializableConflictOut](../C/CheckForSerializableConflictOut.md)

## Notes and Other Information
- Assumes the XID is a top-level transaction ID, not a subtransaction
- The function includes assertions to ensure the XID is valid and different from the current transaction's top XID
- Uses efficient binary search (pg_lfind32) to check if an XID is in the snapshot's active transaction array
- This function is part of the infrastructure that enables PostgreSQL's serializable snapshot isolation level to detect dangerous structures (rw-conflicts) that could lead to serialization anomalies
- Located at src/backend/storage/lmgr/predicate.c:3962

## Simplified Source

```c
static bool
XidIsConcurrent(TransactionId xid)
{
    Snapshot snap;

    Assert(TransactionIdIsValid(xid));
    Assert(!TransactionIdEquals(xid, GetTopTransactionIdIfAny()));

    snap = GetTransactionSnapshot();

    // Transaction committed before our snapshot
    if (TransactionIdPrecedes(xid, snap->xmin))
        return false;

    // Transaction started after our snapshot
    if (TransactionIdFollowsOrEquals(xid, snap->xmax))
        return true;

    // Check if transaction is in our active list
    return pg_lfind32(xid, snap->xip, snap->xcnt);
}
```