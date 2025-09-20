# ProcArrayEndTransactionInternal

## Location
[src/backend/storage/ipc/procarray.c:731-791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L731-L791)

## Overview
Internal function that performs the actual work of marking a write transaction as no longer running, assuming appropriate locks are already held.

## Definition

```c
static inline void
ProcArrayEndTransactionInternal(PGPROC *proc, TransactionId latestXid)
```
## Detailed Description
ProcArrayEndTransactionInternal is the low-level worker function that performs the actual transaction cleanup in the shared process array. Unlike its public counterpart ProcArrayEndTransaction, this function assumes that the caller has already acquired the necessary locks (specifically ProcArrayLock in exclusive mode).

The function performs comprehensive cleanup of transaction-related state:
1. Clears the transaction ID from both the global xids array and the process's local xid field
2. Resets various transaction-related flags and identifiers
3. Clears subtransaction information from both local and global structures
4. Updates global transaction completion tracking
5. Advances the latestCompletedXid to maintain proper transaction visibility

This function is designed to be called from multiple contexts: directly when locks can be acquired immediately, or as part of group transaction clearing when lock contention is high.

## Parameters / Member Variables
- : Pointer to the PGPROC structure of the transaction being ended
- : The latest transaction ID among the main XID and subtransactions

## Dependencies
- Functions called/Symbols referenced:
  - LWLockHeldByMeInMode
  - TransactionIdIsValid
  - [MaintainLatestCompletedXid](../M/MaintainLatestCompletedXid.md)
  - InvalidTransactionId
  - InvalidLocalTransactionId
  - PROC_VACUUM_STATE_MASK
  - ProcGlobal
  - TransamVariables

- Called from (representative examples):
  - [ProcArrayEndTransaction](ProcArrayEndTransaction.md)
  - [ProcArrayGroupClearXid](ProcArrayGroupClearXid.md)
  - xc_slow_answer_inc

## Notes and Other Information
- This is a static inline function designed for internal use within the procarray module
- Requires ProcArrayLock to be held in exclusive mode by the caller
- Performs extensive assertions to verify proper locking and state consistency
- Clears both local PGPROC fields and corresponding global arrays atomically
- Special handling for vacuum-related status flags to avoid unnecessary cache line dirtying
- Subtransaction cleanup includes both count and overflow state
- Updates global transaction completion counters for monitoring and statistics
- The function assumes the transaction has already been committed or aborted in WAL
- Designed to minimize shared memory updates when possible (e.g., only clearing vacuum flags if they were set)