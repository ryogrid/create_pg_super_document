# PlannedStmtRequiresSnapshot

## Location
[src/backend/tcop/pquery.c:1718-1765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L1718-L1765)

## Overview
PlannedStmtRequiresSnapshot determines whether a planned statement requires an MVCC snapshot to execute correctly.

## Definition

```c
enumerate those that
	 * do not need one.
	 *
	 * Transaction control, LOCK, and SET must *not* set a snapshot, since
	 * they need to be executable at the start of a transaction-snapshot-mode
	 * transaction without freezing a snapshot.  By extension we allow SHOW
	 * not to set a snapshot.  The other stmts listed are just efficiency
	 * hacks.  Beware of listing anything that can modify the database --- if,
	 * say, it has to update an index with expressions that invoke
	 * user-defined functions, then it had better have a snapshot.
	 */
	if (IsA(utilityStmt, TransactionStmt) ||
		IsA(utilityStmt, LockStmt) ||
		IsA(utilityStmt, VariableSetStmt) ||
		IsA(utilityStmt, VariableShowStmt) ||
		IsA(utilityStmt, ConstraintsSetStmt) ||
	/* efficiency hacks from here down */
		IsA(utilityStmt, FetchStmt) ||
		IsA(utilityStmt, ListenStmt) ||
		IsA(utilityStmt, NotifyStmt) ||
		IsA(utilityStmt, UnlistenStmt) ||
		IsA(utilityStmt, CheckPointStmt))
		return false;
```
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
  - [PlannedStmt](PlannedStmt.md) structure and its utilityStmt field
  - [Node](../N/Node.md) type checking (IsA macro)
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
  - [PortalRunUtility](PortalRunUtility.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)

## Notes and Other Information
- Returns true for all non-utility statements (DML operations)
- Uses a whitelist approach for statements that don't need snapshots, ensuring safety
- Critical for proper transaction isolation and MVCC behavior
- The function's logic balances correctness with performance by identifying statements that can safely execute without snapshots
- New utility statement types should default to requiring snapshots unless explicitly exempted
- Transaction control statements must be exempt to avoid snapshot conflicts at transaction boundaries