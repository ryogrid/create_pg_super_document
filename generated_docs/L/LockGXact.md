# LockGXact

## Location
[src/backend/access/transam/twophase.c:552-627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L552-L627)

## Overview
Locates a prepared transaction by its Global Identifier (GID) and locks it for exclusive access during COMMIT PREPARED or ROLLBACK PREPARED operations.

## Definition


## Detailed Description
LockGXact is responsible for finding and securing exclusive access to a prepared transaction identified by its GID. The function performs comprehensive validation including checking transaction validity, ownership permissions, database context, and current lock status. It ensures that only the transaction owner or a superuser can access the prepared transaction, and that the operation is performed from the correct database context. Once validated, it marks the transaction as locked to prevent concurrent access and registers the current backend as the locking owner.

## Parameters / Member Variables
- : The Global Identifier string of the prepared transaction to lock
- : The Object ID of the user attempting to lock the transaction

## Dependencies
- Functions called/Symbols referenced:
  - [AtProcExit_Twophase](../A/AtProcExit_Twophase.md)
  - [before_shmem_exit](../b/before_shmem_exit.md)
  - GlobalTransaction
  - [PGPROC](../P/PGPROC.md)
  - GetPGProcByNumber
  - INVALID_PROC_NUMBER
  - superuser_arg
- Called from (representative examples):
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)

## Notes and Other Information
- This is a static function used internally within the two-phase commit system
- Registers the AtProcExit_Twophase exit hook on first call to ensure cleanup
- Enforces strict access control: only the transaction owner or superuser can lock the transaction
- Requires operations to be performed from the same database where the transaction was prepared
- Sets MyLockedGxact to track the locked transaction for cleanup purposes
- Prevents concurrent COMMIT PREPARED or ROLLBACK PREPARED operations on the same transaction
- Returns the locked GlobalTransaction structure or throws an error if the transaction cannot be found or accessed
- Uses TwoPhaseStateLock for thread-safe access to the prepared transaction array