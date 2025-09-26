# TwoPhaseGetDummyProc

## Location
[src/backend/access/transam/twophase.c:918-937](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L918-L937)

## Overview
Retrieves the PGPROC structure that represents a prepared transaction identified by its transaction ID, providing access to process-level information for two-phase commit operations.

## Definition
PGPROC *TwoPhaseGetDummyProc(TransactionId xid, bool lock_held)

## Detailed Description
This function returns a pointer to the PGPROC structure associated with a prepared transaction specified by its XID. The PGPROC structure contains essential process information such as locks, wait states, and transaction context. For prepared transactions, these "dummy" PGPROC entries maintain the transaction's process state even after the original backend process has disconnected, which is crucial for proper two-phase commit protocol implementation.

The function works by first calling TwoPhaseGetGXact to locate the global transaction, then using GetPGProcByNumber to retrieve the corresponding PGPROC structure based on the stored pgprocno.

## Parameters / Member Variables
- `xid`: TransactionId of the prepared transaction to look up
- `lock_held`: Boolean flag indicating whether the caller already holds TwoPhaseStateLock; if true, the function will not acquire the lock

## Dependencies
- Functions called/Symbols referenced:
  - [TwoPhaseGetGXact](TwoPhaseGetGXact.md)
  - [GlobalTransaction](../G/GlobalTransaction.md)
  - GetPGProcByNumber
  - [FullTransactionId](../F/FullTransactionId.md)
- Called from (representative examples):
  - [PostPrepare_Locks](../P/PostPrepare_Locks.md)
  - [lock_twophase_recover](../l/lock_twophase_recover.md)  
  - [lock_twophase_postcommit](../l/lock_twophase_postcommit.md)

## Notes and Other Information
- The returned PGPROC represents a "dummy" process that maintains state for the prepared transaction after the original backend disconnects
- This is essential for lock management and wait queue operations during prepared transaction lifetime
- The lock_held parameter allows for optimized calling patterns when the caller already holds necessary locks
- Primarily used by the lock manager for handling locks associated with prepared transactions