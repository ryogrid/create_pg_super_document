# AbortCurrentTransaction

## Location
[src/backend/access/transam/xact.c:3387-3404](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xact.c#L3387-L3404)

## Overview
AbortCurrentTransaction is a wrapper function that handles the complete abortion of the current transaction and any nested subtransactions through an iterative approach to prevent dangerous recursion.

## Definition
```c
void AbortCurrentTransaction(void)
```

## Detailed Description
AbortCurrentTransaction serves as a safe wrapper around the core transaction abort logic implemented in AbortCurrentTransactionInternal. Similar to CommitTransactionCommand, this function's primary purpose is to prevent potentially dangerous recursion that could occur when handling complex transaction structures involving nested subtransactions.

The function implements an iterative approach, repeatedly calling AbortCurrentTransactionInternal until all abort-related work is completed. This design ensures that deeply nested subtransaction hierarchies are properly unwound and aborted without risking stack overflow from recursive calls. The loop continues until AbortCurrentTransactionInternal returns true, indicating that all transaction and subtransaction abort work has been successfully completed.

This wrapper is essential for PostgreSQL's robust error handling and recovery mechanisms, ensuring that failed transactions are properly cleaned up regardless of their complexity or nesting level.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [AbortCurrentTransactionInternal](AbortCurrentTransactionInternal.md)
- Called from (representative examples):
  - _SPI_commit (SPI error handling)
  - [_SPI_rollback](../S/_SPI_rollback.md) (SPI transaction rollback)
  - [PostgresMain](../P/PostgresMain.md) (main query processing error recovery)
  - [pa_stream_abort](../p/pa_stream_abort.md) (logical replication error handling)
  - Various replication components (reorderbuffer, snapbuild)

## Notes and Other Information
- Located in src/backend/access/transam/xact.c:3387-3404
- Prevents stack overflow from recursive subtransaction abort processing
- Critical component of PostgreSQL's error recovery and transaction cleanup system
- Works in conjunction with AbortCurrentTransactionInternal's comprehensive state machine
- Used extensively in error handling paths throughout PostgreSQL
- Ensures all levels of nested subtransactions are properly aborted and cleaned up
- The iterative design mirrors CommitTransactionCommand's approach for consistency
- Essential for maintaining database consistency when transactions fail at any level
- Called from both user-facing operations and internal system processes