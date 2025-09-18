# ProcReleaseLocks

## Location
src/backend/storage/lmgr/proc.c: 811 - 827

## Overview
Releases locks associated with the current transaction at main transaction commit or abort, handling different lock types and scopes based on the transaction outcome.

## Definition


## Detailed Description
ProcReleaseLocks is a high-level lock cleanup function that manages the release of various types of locks when a main transaction completes, either through commit or abort. The function implements different release strategies based on the transaction outcome:

For transaction commits:
- Releases standard locks except session-level locks
- Releases only transaction-level advisory locks, preserving session-level advisory locks

For transaction aborts:
- Releases all locks including session-level locks
- Releases transaction-level advisory locks

The function also performs error cleanup by ensuring any pending lock waits are properly cancelled before proceeding with lock releases. It operates only on main transactions and does not handle subtransaction scenarios directly.

## Parameters / Member Variables
- : Boolean flag indicating whether this is being called from a transaction commit (true) or abort (false)

## Dependencies
- Functions called/Symbols referenced:
  - [LockErrorCleanup](../L/LockErrorCleanup.md)
  - [LockReleaseAll](../L/LockReleaseAll.md)
  - DEFAULT_LOCKMETHOD
  - USER_LOCKMETHOD
- Called from (representative examples):
  - [ResourceOwnerReleaseInternal](../R/ResourceOwnerReleaseInternal.md)

## Notes and Other Information
- The function is only relevant for main transactions; subtransaction commits defer lock releasing to the parent transaction
- Subtransaction aborts use the ResourceOwner mechanism for retail lock releasing rather than this function
- Advisory locks have different scopes: transaction-level locks are always released, while session-level advisory locks persist across transaction boundaries
- The function safely handles cases where MyProc is NULL (no current process)
- Always calls LockErrorCleanup first to handle any pending lock waits that might exist due to error conditions