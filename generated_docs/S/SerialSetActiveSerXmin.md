# SerialSetActiveSerXmin

## Location
[src/backend/storage/lmgr/predicate.c:990-1040](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/predicate.c#L990-L1040)

## Overview
SerialSetActiveSerXmin updates the minimum transaction ID for active serializable transactions, allowing the system to discard information about older transactions that are no longer relevant for conflict detection.

## Definition
```c
static void SerialSetActiveSerXmin(TransactionId xid)
```

## Detailed Description
This function manages the tail boundary of the serial control structure by setting the new minimum transaction ID (xmin) for active serializable transactions. The function handles three distinct scenarios:

1. **No Active Transactions**: When an invalid transaction ID is passed, it sets both tailXid and headXid to invalid, indicating no serializable transactions are currently active.

2. **Recovery Mode**: During recovery of prepared transactions, the global xmin might move backwards depending on recovery order. The function allows this normally invalid condition since no serializable transactions will commit during recovery.

3. **Normal Operation**: Updates the tailXid to the new minimum, with an assertion that the new xmin should follow (be greater than) the current tailXid.

The function maintains the serial control structure's consistency by properly managing the range of valid transaction IDs in the SLRU cache, enabling efficient garbage collection of obsolete serialization information.

## Parameters / Member Variables
- `xid`: The new minimum transaction ID for active serializable transactions. InvalidTransactionId indicates no active serializable transactions.

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease
  - TransactionIdIsValid
  - [RecoveryInProgress](../R/RecoveryInProgress.md)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [TransactionIdFollows](../T/TransactionIdFollows.md)
- Called from (representative examples):
  - [GetSerializableTransactionSnapshotInt](../G/GetSerializableTransactionSnapshotInt.md)
  - [SetNewSxactGlobalXmin](SetNewSxactGlobalXmin.md)
  - predicatelock_twophase_recover

## Notes and Other Information
- This is a static function, only accessible within the predicate.c file
- Critical for garbage collection of serialization information - allows discarding data for transactions that precede the new xmin
- Special handling during recovery allows backwards movement of xmin, which is normally prohibited
- The function uses exclusive locking on SerialControlLock to ensure atomic updates to the serial control structure
- Part of PostgreSQL's serializable snapshot isolation implementation for maintaining transaction conflict information