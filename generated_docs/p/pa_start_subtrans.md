# pa_start_subtrans

## Location
[src/backend/replication/logical/applyparallelworker.c:1362-1401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1362-L1401)

## Overview
pa_start_subtrans manages subtransaction initialization in parallel apply workers by defining savepoints for new subtransactions in PostgreSQL's logical replication system.

## Definition
void pa_start_subtrans(TransactionId current_xid, TransactionId top_xid)

## Detailed Description
This function handles the creation of subtransactions in parallel apply workers when processing logical replication streams. It detects when a new subtransaction has started by comparing the current transaction ID with the top-level transaction ID and checking if the current XID is already in the tracked subtransaction list.

When a new subtransaction is detected, the function performs several critical operations:

1. **Savepoint Creation**: Generates a unique savepoint name using pa_savepoint_name() and defines it using DefineSavepoint()
2. **Transaction Block Management**: Ensures the worker is in a proper transaction block state by starting one if necessary
3. **Subtransaction Tracking**: Adds the new transaction ID to the subxactlist for future reference
4. **Memory Context Management**: Uses TopTransactionContext to ensure the subtransaction list persists for the transaction's lifetime

The function handles the complex PostgreSQL transaction state transitions required to properly establish savepoints within transaction blocks, ensuring that rollback operations can be performed if needed during streaming logical replication.

## Parameters / Member Variables
- : The transaction ID of the current change being processed, which may represent a subtransaction
- : The top-level transaction ID that encompasses all related subtransactions

## Dependencies
- Functions called/Symbols referenced:
  - [list_member_xid](../l/list_member_xid.md) (check if XID exists in list)
  - [pa_savepoint_name](pa_savepoint_name.md) (generate unique savepoint name)
  - [IsTransactionBlock](../I/IsTransactionBlock.md) (check transaction block state)
  - [IsTransactionState](../I/IsTransactionState.md) (verify transaction state)
  - [StartTransactionCommand](../S/StartTransactionCommand.md) (initiate transaction)
  - [BeginTransactionBlock](../B/BeginTransactionBlock.md) (start transaction block)
  - [CommitTransactionCommand](../C/CommitTransactionCommand.md) (commit transaction command)
  - [DefineSavepoint](../D/DefineSavepoint.md) (create named savepoint)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md) (memory context management)
  - lappend_xid (append XID to list)
  - MySubscription (global subscription info)
  - TopTransactionContext (transaction memory context)
  - subxactlist (global subtransaction list)
- Called from (representative examples):
  - [handle_streamed_transaction](../h/handle_streamed_transaction.md)

## Notes and Other Information
- This function is located in src/backend/replication/logical/applyparallelworker.c:1362-1401
- Critical for maintaining transaction isolation and rollback capabilities in parallel logical replication
- Handles complex PostgreSQL transaction state management to ensure proper savepoint creation
- Uses memory context switching to ensure subtransaction tracking data persists appropriately
- The function includes detailed transaction block state checking and initialization
- Integrates with PostgreSQL's standard transaction management infrastructure
- Essential for handling nested transactions in streaming logical replication scenarios