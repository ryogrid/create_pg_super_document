# PrepareTransactionBlock

## Location
src/backend/access/transam/xact.c: 3941 - 3992

## Overview
PrepareTransactionBlock implements the SQL PREPARE TRANSACTION command by initiating the two-phase commit process, transitioning the transaction block state to TBLOCK_PREPARE for later completion.

## Definition


## Detailed Description
This function handles the execution of a PREPARE TRANSACTION command, which is part of PostgreSQL's two-phase commit protocol. It first calls EndTransactionBlock(false) to end the current transaction block, then if successful, transitions the outermost transaction state to TBLOCK_PREPARE. The function stores the global transaction identifier (GID) in TopTransactionContext for later retrieval by PrepareTransaction(). The design separates the block state management from the actual prepare work to avoid memory context and resource owner complications during Portal execution. The function returns true if the prepare will proceed, false if it was rolled back or not applicable.

## Parameters / Member Variables
- `gid`: Global transaction identifier string for the prepared transaction

## Dependencies
- Functions called/Symbols referenced:
  - TransactionState (type definition)
  - EndTransactionBlock (transaction block management function)
  - CurrentTransactionState (global variable)
  - MemoryContextStrdup (memory allocation function)
  - TopTransactionContext (memory context)
  - prepareGID (global variable for storing GID)
  - TBlockState enumeration values (TBLOCK_END, TBLOCK_PREPARE, etc.)
- Called from (representative examples):
  - apply_handle_prepare_internal (at src/backend/replication/logical/worker.c:1103)
  - standard_ProcessUtility (at src/backend/tcop/utility.c:640)

## Notes and Other Information
- Returns true for successful PREPARE, false for ROLLBACK or no-op cases
- Actual prepare work is deferred to PrepareTransaction() to avoid Portal execution complications
- Walks up the transaction state stack to find the outermost transaction
- Stores GID in TopTransactionContext to survive until PrepareTransaction() execution
- Part of PostgreSQL's two-phase commit protocol implementation
- Handles edge cases where no transaction is active by returning false without error
- The function design separates state transition from actual prepare logic for architectural reasons