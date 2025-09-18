# PostPrepare_MultiXact

## Location
src/backend/access/transam/multixact.c: 1842 - 1890

## Overview
Performs cleanup and state transfer after a successful PREPARE TRANSACTION operation in two-phase commit.

## Definition
```c
void PostPrepare_MultiXact(TransactionId xid)
```

## Detailed Description
This function handles the transfer of MultiXact state from the current backend process to a dummy process slot reserved for the prepared transaction. It transfers the OldestMemberMXactId from the current process to the prepared transaction's dummy slot while acquiring appropriate locks to ensure atomic visibility of the changes. The function also resets the current process's MultiXact tracking variables and cleans up the local MultiXact cache, similar to transaction end cleanup.

## Parameters / Member Variables
- `xid`: The transaction ID of the prepared transaction

## Dependencies
- Functions called/Symbols referenced:
  - MultiXactIdIsValid
  - [TwoPhaseGetDummyProcNumber](../T/TwoPhaseGetDummyProcNumber.md)
  - LWLockAcquire (MultiXactGenLock, LW_EXCLUSIVE)
  - LWLockRelease
  - InvalidMultiXactId (constant)
  - [dclist_init](../d/dclist_init.md)
- Global variables modified:
  - OldestMemberMXactId[dummyProcNumber]
  - OldestMemberMXactId[MyProcNumber]
  - OldestVisibleMXactId[MyProcNumber]
  - MXactContext
  - MXactCache
- Called from (representative examples):
  - [PrepareTransaction](PrepareTransaction.md)

## Notes and Other Information
- Only transfers OldestMemberMXactId, not OldestVisibleMXactId (prepared transactions don't need visibility tracking)
- Uses locking to ensure atomic visibility of state transfer changes
- Cleans up local cache similar to AtEOXact_MultiXact
- The transferred state will persist until the prepared transaction is committed or aborted
- Critical for maintaining MultiXact consistency across prepare/commit phases
- Part of PostgreSQL's two-phase commit protocol
- Located in src/backend/access/transam/multixact.c:1842-1890