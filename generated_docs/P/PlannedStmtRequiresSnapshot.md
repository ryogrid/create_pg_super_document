# PlannedStmtRequiresSnapshot

## Location
src/backend/tcop/pquery.c: 1718 - 1765

## Overview
PlannedStmtRequiresSnapshot determines whether a planned statement requires an MVCC snapshot to execute correctly.

## Definition


## Detailed Description
PlannedStmtRequiresSnapshot analyzes a planned statement to determine if it requires a snapshot for execution. The function implements PostgreSQL's snapshot management policy by distinguishing between statements that need consistent view of the database and those that operate at a different level.

The function follows a conservative approach: most statements require snapshots, and only specific utility statements that explicitly don't need them are exempted. This ensures transaction consistency and proper MVCC behavior.

Non-DML statements (queries, inserts, updates, deletes) always require snapshots since they need a consistent view of data. For utility statements, the function categorizes them into two groups:
1. Statements that must NOT have snapshots (transaction control, locking, variable setting)
2. Statements that don't need snapshots for efficiency (notification commands, administrative operations)

Transaction control statements (BEGIN, COMMIT, ROLLBACK) cannot have snapshots because they need to execute at the start of transactions before a snapshot is established. Similarly, LOCK and SET commands need to work in snapshot-free contexts.

## Parameters / Member Variables
- : The planned statement to analyze for snapshot requirements

## Dependencies
- Functions called/Symbols referenced:
  - PlannedStmt structure and its utilityStmt field
  - Node type checking (IsA macro)
  - Various utility statement types:
    - TransactionStmt
    - LockStmt
    - VariableSetStmt
    - VariableShowStmt
    - ConstraintsSetStmt
    - FetchStmt
    - ListenStmt
    - NotifyStmt
    - UnlistenStmt
    - CheckPointStmt
- Called from (representative examples):
  - PortalRunUtility
  - _SPI_execute_plan

## Notes and Other Information
- Returns true for all non-utility statements (DML operations)
- Uses a whitelist approach for statements that don't need snapshots, ensuring safety
- Critical for proper transaction isolation and MVCC behavior
- The function's logic balances correctness with performance by identifying statements that can safely execute without snapshots
- New utility statement types should default to requiring snapshots unless explicitly exempted
- Transaction control statements must be exempt to avoid snapshot conflicts at transaction boundaries