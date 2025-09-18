# TransactionStateData

## Location
[src/backend/access/transam/xact.c:191-216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L191-L216)

## Overview
TransactionStateData is a core structure that maintains the complete state information for a PostgreSQL transaction, including nested subtransactions, savepoints, and parallel execution context.

## Definition


## Detailed Description
TransactionStateData serves as the comprehensive state container for PostgreSQL's transaction management system. It tracks both low-level transaction states and high-level block states, manages nested subtransactions through a parent-child relationship, and maintains context for parallel execution modes. The structure supports PostgreSQL's sophisticated transaction nesting capabilities, including savepoints, and ensures proper resource management across transaction boundaries.

## Parameters / Member Variables
- : The complete transaction identifier for this transaction
- : Identifier for subtransaction within the main transaction
- : Optional savepoint name for named savepoints
- : Numeric level indicating savepoint nesting depth
- : Low-level transaction state (TransState enum)
- : High-level transaction block state (TBlockState enum)
- : Depth of transaction nesting
- : GUC (Grand Unified Configuration) context nesting level
- : Memory context specific to this transaction's lifetime
- : Resource owner managing query-level resources
- : Array of committed child transaction IDs, maintained in XID order
- : Current number of child transaction IDs in the array
- : Allocated capacity of the childXids array
- : Previous user ID before transaction started
- : Previous security restriction context
- : Read-only state when transaction began
- : Flag indicating if transaction started during recovery
- : Whether transaction ID has been written to WAL
- : Counter for Enter/ExitParallelMode calls
- : Whether any parent transaction is executing in parallel mode
- : Flag to automatically start new transaction block after current one
- : For subtransactions, whether top-level XID is logged
- : Back-reference to parent transaction state for nested transactions

## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId
  - SubTransactionId
  - TransState
  - TBlockState
  - ResourceOwner
- Called from (representative examples):
  - TransactionState (typedef)
  - SerializedTransactionStateHeaderSize
  - [PushTransaction](../P/PushTransaction.md)

## Notes and Other Information
The parallelModeLevel field counts unmatched EnterParallelMode calls at this transaction level, while parallelChildXact tracks if any upper transaction level has nonzero parallelModeLevel. This design enables proper parallel execution context management across nested transactions. The structure forms a linked list through the parent pointer, allowing PostgreSQL to maintain a complete transaction stack for proper rollback and resource cleanup operations.