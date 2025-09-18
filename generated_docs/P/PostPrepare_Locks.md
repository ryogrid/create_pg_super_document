# PostPrepare_Locks

## Location
src/backend/storage/lmgr/lock.c: 3400 - 3583

## Overview
PostPrepare_Locks transfers ownership of transaction-level locks from the current process to a dummy PGPROC associated with a prepared transaction after successful PREPARE.

## Definition
```c
void PostPrepare_Locks(TransactionId xid)
```

## Detailed Description
This function is called after a successful PREPARE TRANSACTION to transfer lock ownership from the current backend process to a dummy PGPROC structure that represents the prepared transaction. It performs two main phases:

**Phase 1 - Local Lock Cleanup:**
- Scans the local lock table (LOCALLOCK entries) to identify transaction-level locks
- Marks the release mask in corresponding PROCLOCK entries to indicate which lock modes need to be transferred
- Removes LOCALLOCK entries to clean up the backend's local state
- Skips session-level locks and VXID locks

**Phase 2 - Lock Ownership Transfer:**
- Iterates through each lock partition to find PROCLOCK entries owned by the current process
- Uses hash_update_hash_key() to reassign PROCLOCK ownership from the current process to the dummy PGPROC
- Updates the proclock's chain linkage to move it from the current process to the prepared transaction
- Maintains all lock state information while changing ownership

The entire operation runs in a critical section to ensure atomicity and consistency.

## Parameters / Member Variables
- `xid`: TransactionId of the prepared transaction that will own the transferred locks

## Dependencies
- Functions called/Symbols referenced:
  - [TwoPhaseGetDummyProc](../T/TwoPhaseGetDummyProc.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [RemoveLocalLock](../R/RemoveLocalLock.md)
  - LOCKBIT_ON
  - LockHashPartitionLockByIndex
  - dlist_foreach_modify
  - dlist_container
  - [hash_update_hash_key](../h/hash_update_hash_key.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - START_CRIT_SECTION/END_CRIT_SECTION
- Called from (representative examples):
  - [PrepareTransaction](PrepareTransaction.md)

## Notes and Other Information
- The function operates within a critical section to prevent interruption during lock transfer
- Lock group leaders cannot be prepared - only individual processes or group leaders themselves
- Virtual transaction (VXID) locks are excluded from transfer as they are not meaningful after restart
- The function assumes that fast-path locks were already moved to the main table during AtPrepare_Locks()
- [PROCLOCK](PROCLOCK.md) hash keys are updated in-place rather than creating new entries to avoid out-of-memory issues
- After transfer, the dummy PGPROC will hold all the locks until COMMIT PREPARED or ROLLBACK PREPARED
- Dangling pointers in the transaction's resource owner are acceptable since resowner.c doesn't free locks at toplevel commit/abort
- The releaseMask and holdMask should be equal for all locks being transferred (no partial releases)