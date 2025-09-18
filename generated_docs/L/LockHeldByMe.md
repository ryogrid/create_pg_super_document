# LockHeldByMe

## Location
src/backend/storage/lmgr/lock.c: 590 - 631

## Overview
A function that tests whether the current transaction holds a specific lock, with optional checking for stronger lock modes.

## Definition
bool LockHeldByMe(const LOCKTAG *locktag, LOCKMODE lockmode, bool orstronger)

## Detailed Description
LockHeldByMe checks if the current transaction holds a lock on the specified resource at the specified lock mode or stronger. The function searches the backend's local lock hash table (LOCALLOCK entries) to determine lock ownership. When the orstronger parameter is true, the function will also return true if the current transaction holds a stronger (numerically higher) lock mode on the same resource.

The function first constructs a LOCALLOCKTAG and searches for a corresponding LOCALLOCK entry. If found and the lock count is positive, it confirms the lock is held. For stronger mode checking, it recursively calls itself with progressively stronger lock modes up to MaxLockMode.

## Parameters / Member Variables
- locktag: Pointer to the LOCKTAG identifying the resource to check
- lockmode: The lock mode to test for
- orstronger: If true, also accept stronger lock modes as satisfying the check

## Dependencies
- Functions called/Symbols referenced:
  - MemSet: Clears the LOCALLOCKTAG structure padding
  - [hash_search](../h/hash_search.md): Searches the local lock hash table for the lock entry
  - LockMethodLocalHash: The backend's local lock hash table
  - HASH_FIND: Hash operation flag for searching
  - MaxLockMode: Maximum valid lock mode for iteration
  - [LockHeldByMe](LockHeldByMe.md): Recursive call for checking stronger modes
- Called from (representative examples):
  - [CheckRelationLockedByMe](../C/CheckRelationLockedByMe.md): High-level relation lock checking
  - [CheckRelationOidLockedByMe](../C/CheckRelationOidLockedByMe.md): OID-based relation lock checking
  - [check_lock_if_inplace_updateable_rel](../c/check_lock_if_inplace_updateable_rel.md): Heap access method lock validation
  - [check_inplace_rel_lock](../c/check_inplace_rel_lock.md): In-place update lock verification
  - [UpdateSubscriptionRelStateEx](../U/UpdateSubscriptionRelStateEx.md): Subscription relation lock checking
  - LockHashPartitionLockByProc: Lock partition operations

## Notes and Other Information
- Only checks the current backend's local lock table, not the shared lock table
- The 'stronger' relationship is defined numerically (higher mode number = stronger)
- Uses recursive calls when checking for stronger modes, which is acceptable given the small number of lock modes
- Essential for preventing lock-related errors and ensuring proper lock semantics
- The nLocks > 0 check ensures the lock is actually held (not just reserved)
- Properly handles structure padding by zeroing the entire LOCALLOCKTAG