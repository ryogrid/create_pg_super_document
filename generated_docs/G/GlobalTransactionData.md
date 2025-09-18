# GlobalTransactionData

## Location
src/backend/access/transam/twophase.c: 147 - 170

## Overview
GlobalTransactionData is a structure that describes one global transaction that is in prepared state or attempting to become prepared in PostgreSQL's two-phase commit protocol.

## Definition


## Detailed Description
This structure manages the complete lifecycle of a global transaction in PostgreSQL's two-phase commit protocol. The lifecycle follows these phases:

1. **Initial Setup**: After verifying the GID is not in use, an entry is created in the TwoPhaseState->prepXacts array with valid = false, locked by the current backend.

2. **Preparation Complete**: After successful preparation, valid is set to true and the associated PGPROC is entered into the global ProcArray.

3. **Commit/Rollback Initiation**: To begin COMMIT PREPARED or ROLLBACK PREPARED, the entry must be valid and unlocked, then gets locked by storing the current proc number in locking_backend.

4. **Completion**: Upon finishing COMMIT PREPARED or ROLLBACK PREPARED, the entry is removed from ProcArray and prepXacts array, returning to the freelist.

The structure maintains critical WAL logging information through start and end LSNs to ensure proper recovery and commit ordering.

## Parameters / Member Variables
- : Pointer to next GlobalTransaction in the free list for memory management
- : ID of the associated dummy PGPROC entry used for process management
- : Timestamp when the transaction was prepared
- : WAL log sequence number where the prepare record starts (needed for reading state data during commit)
- : WAL log sequence number where the prepare record ends (needed for waiting before commit)
- : The transaction ID of this global transaction
- : Object ID of the user who executed this transaction
- : Process number of the backend currently working on this transaction (prevents concurrent operations)
- : Boolean flag indicating if the PGPROC entry is properly registered in the process array
- : Boolean flag indicating if the prepare state file has been written to disk
- : Boolean flag indicating if this entry was created during WAL replay/recovery
- : Character array storing the Global Identifier assigned to this prepared transaction

## Dependencies
- Functions called/Symbols referenced:
  - GlobalTransaction (typedef pointer)
  - ProcNumber (process identification type)
  - GIDSIZE (constant defining GID string size)
- Called from (representative examples):
  - TwoPhaseShmemSize (for shared memory size calculation)
  - GetPreparedTransactionList (for retrieving prepared transaction information)

## Notes and Other Information
- This structure is central to PostgreSQL's distributed transaction support
- The dual LSN tracking (start/end) is crucial for proper WAL-based recovery of prepared transactions
- The locking mechanism via locking_backend prevents race conditions during concurrent commit/rollback attempts
- Failed preparations between setup and completion must be properly cleaned up via AtAbort_Twophase()
- The structure supports both normal operation and crash recovery scenarios through the inredo flag