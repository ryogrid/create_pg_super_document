# CommitTransactionCommand

## Location
src/backend/access/transam/xact.c: 3093 - 3110

## Overview
CommitTransactionCommand is a wrapper function that handles the iterative process of committing transactions and subtransactions, preventing dangerous recursion in CommitTransactionCommandInternal.

## Definition
```c
void CommitTransactionCommand(void)
```

## Detailed Description
CommitTransactionCommand serves as a safe wrapper around the core transaction commit logic implemented in CommitTransactionCommandInternal. The function's primary purpose is to prevent potentially dangerous recursion that could occur when handling complex transaction structures involving subtransactions.

The function implements a simple iterative approach, repeatedly calling CommitTransactionCommandInternal until all transaction-related work is completed. This design ensures that nested subtransactions are properly handled without risking stack overflow from recursive calls. The loop continues until CommitTransactionCommandInternal returns true, indicating that all transaction commit work has been successfully completed.

This wrapper pattern is essential for handling PostgreSQL's hierarchical transaction model, where transactions can contain multiple levels of subtransactions that must be committed in the proper order and manner.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [CommitTransactionCommandInternal](CommitTransactionCommandInternal.md)
- Called from (representative examples):
  - [finish_xact_command](../f/finish_xact_command.md) (main transaction processing)
  - [InitPostgres](../I/InitPostgres.md) (initialization contexts)
  - [vacuum_rel](../v/vacuum_rel.md) (vacuum operations)
  - _SPI_commit (SPI transaction handling)
  - Various replication workers (logical replication)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (parallel processing)
  - Multiple DDL operations (index creation, table operations)

## Notes and Other Information
- Located in src/backend/access/transam/xact.c:3093-3110
- Prevents stack overflow from recursive subtransaction processing
- Used extensively throughout PostgreSQL for safe transaction commit operations
- The iterative approach ensures all subtransactions are properly handled
- Critical for maintaining transaction integrity in complex nested transaction scenarios
- Called in many contexts including DDL operations, replication, vacuum, and system initialization
- The function's simplicity belies its importance in PostgreSQL's transaction management architecture