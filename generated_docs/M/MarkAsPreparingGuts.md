# MarkAsPreparingGuts

## Location
[src/backend/access/transam/twophase.c:433-503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L433-L503)

## Overview
Initializes a GlobalTransaction structure and its associated PGPROC entry, setting up the internal state required for a two-phase commit transaction in the preparing phase.

## Definition


## Detailed Description
MarkAsPreparingGuts is an internal helper function that performs the low-level initialization of a GlobalTransaction and its corresponding PGPROC entry. It's designed to work both during normal transaction preparation and during crash recovery when reloading prepared transactions. The function initializes the PGPROC structure with appropriate values for a prepared transaction, sets up virtual transaction IDs for lock conflict resolution, and populates the GlobalTransaction structure with metadata. It assumes appropriate locks are already held and operates on pre-allocated structures.

## Parameters / Member Variables
- : The GlobalTransaction structure to initialize
- : The transaction ID being prepared
- : The Global Identifier string for the transaction
- : Timestamp when the transaction was prepared
- : Object ID of the user who owns this prepared transaction
- : Object ID of the database where this transaction is being prepared

## Dependencies
- Functions called/Symbols referenced:
  - GlobalTransaction
  - [PGPROC](../P/PGPROC.md)
  - LWLockHeldByMeInMode
  - GetPGProcByNumber
  - MemSet
  - [dlist_node_init](../d/dlist_node_init.md)
  - PROC_WAIT_STATUS_OK
  - LocalTransactionIdIsValid
  - AmStartupProcess
  - INVALID_PROC_NUMBER
  - LW_WS_NOT_WAITING
  - [pg_atomic_init_u64](../p/pg_atomic_init_u64.md)
  - NUM_LOCK_PARTITIONS
  - [dlist_init](../d/dlist_init.md)
- Called from (representative examples):
  - [MarkAsPreparing](MarkAsPreparing.md)
  - [RecoverPreparedTransactions](../R/RecoverPreparedTransactions.md)

## Notes and Other Information
- This is a static function used internally within the two-phase commit system
- Must be called with TwoPhaseStateLock held in exclusive mode
- Handles both normal operation and crash recovery scenarios
- Initializes PGPROC as a background worker to avoid interference with normal backends
- Sets MyLockedGxact to track ownership of the GlobalTransaction entry
- The subxid data is not populated here and must be filled later by GXactLoadSubxactData
- Clones the virtual transaction ID from the current process when available