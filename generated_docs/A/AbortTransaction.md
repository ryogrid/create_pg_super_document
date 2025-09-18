# AbortTransaction

## Location
[src/backend/access/transam/xact.c:2749-2944](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L2749-L2944)

## Overview
AbortTransaction handles the complete rollback process for a PostgreSQL transaction, performing comprehensive cleanup of all transaction-related resources and state.

## Definition


## Detailed Description
AbortTransaction is responsible for safely aborting a transaction and cleaning up all associated resources. The function handles both regular transactions and parallel worker transactions, performing critical emergency cleanup before systematic resource deallocation.

The abort process follows a carefully designed sequence:
1. **Emergency cleanup**: Immediately releases lightweight locks, clears wait states, resets WAL state, and re-enables signals
2. **State validation**: Verifies the transaction is in an abortable state (TRANS_INPROGRESS or TRANS_PREPARE)  
3. **Security context reset**: Restores user ID and security context to handle cases where abort occurs during SECURITY DEFINER functions
4. **Subsystem cleanup**: Resets various subsystem states (REINDEX, logical streaming, snapshot export)
5. **Abort processing**: Calls abort-specific cleanup routines for triggers, portals, storage, etc.
6. **WAL recording**: Records the transaction abort (except for parallel workers)
7. **Resource release**: Systematically releases resources in the proper order
8. **Final cleanup**: Performs end-of-transaction cleanup for all subsystems

The transaction state transitions to TRANS_ABORT and remains there until CleanupTransaction() is called.

## Parameters / Member Variables
This function takes no parameters but operates on:
- : The current transaction's state structure
- : Used to detect parallel worker mode (TBLOCK_PARALLEL_INPROGRESS)
- : Transaction state that must be TRANS_INPROGRESS or TRANS_PREPARE
- : Transaction ID returned by RecordTransactionAbort()
- : Flag indicating if this is a parallel worker transaction

## Dependencies
- Functions called/Symbols referenced:
  - [AtAbort_Memory](AtAbort_Memory.md)/AtAbort_ResourceOwner (emergency memory/resource cleanup)
  - LWLockReleaseAll (release all lightweight locks immediately)
  - [RecordTransactionAbort](../R/RecordTransactionAbort.md) (record abort in WAL, except for parallel workers)
  - [SetUserIdAndSecContext](../S/SetUserIdAndSecContext.md) (restore user ID and security context)
  - AtEOXact_* functions (end-of-transaction cleanup for various subsystems)
  - AtAbort_* functions (abort-specific cleanup routines)
  - ResourceOwnerRelease (systematic resource cleanup)
  - [ProcArrayEndTransaction](../P/ProcArrayEndTransaction.md) (remove from process array)

- Called from (representative examples):
  - [AbortCurrentTransactionInternal](AbortCurrentTransactionInternal.md) (various error recovery scenarios)
  - [AbortOutOfAnyTransaction](AbortOutOfAnyTransaction.md) (emergency abort from any transaction state)
  - [CommitTransactionCommandInternal](../C/CommitTransactionCommandInternal.md) (when commit preparation fails)

## Notes and Other Information
- Uses HOLD_INTERRUPTS/RESUME_INTERRUPTS to prevent interruption during critical cleanup
- Immediately releases lightweight locks since they might be needed during cleanup
- Restores signal mask early to ensure timeout infrastructure works during abort
- Parallel workers don't record their own abort but nudge WAL-writer for LSN reporting
- Transaction state remains TRANS_ABORT until CleanupTransaction() resets it to TRANS_DEFAULT
- Resource cleanup follows the same ordering principles as CommitTransaction for consistency
- Can skip resource cleanup if the transaction failed before creating a resource owner
- Handles both normal transaction aborts and aborts during two-phase commit preparation