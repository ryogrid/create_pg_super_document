# ExecCheckXactReadOnly

## Location
[src/backend/executor/execMain.c:792-825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L792-L825)

## Overview
Verifies that a planned statement does not imply any writes to non-temporary tables, and enforces additional restrictions in parallel execution mode.

## Definition


## Detailed Description
This function enforces read-only transaction semantics by examining the permission requirements of a planned statement. It prevents write operations to non-temporary tables in read-only transactions and enforces stricter rules in parallel execution mode where even temporary table writes are prohibited.

The function iterates through all permission information in the planned statement and checks for any permissions beyond SELECT. For non-temporary tables, any write permissions trigger a read-only violation. The function also handles special cases like modifying CTEs (Common Table Expressions) which are considered write operations even in SELECT statements.

In parallel mode, the function is more restrictive and prevents any command that is not a pure SELECT or has modifying CTEs, since parallel workers cannot safely perform write operations.

## Parameters / Member Variables
- : Pointer to PlannedStmt structure containing:
  - : List of RTEPermissionInfo structures describing required permissions
  - : Type of SQL command (CMD_SELECT, CMD_INSERT, etc.)
  - : Boolean indicating presence of data-modifying CTEs

## Dependencies
- Functions called/Symbols referenced:
  - [isTempNamespace](../i/isTempNamespace.md)
  - [get_rel_namespace](../g/get_rel_namespace.md)
  - [PreventCommandIfReadOnly](../P/PreventCommandIfReadOnly.md)
  - [PreventCommandIfParallelMode](../P/PreventCommandIfParallelMode.md)
  - CreateCommandName
  - lfirst_node
- Called from (representative examples):
  - [standard_ExecutorStart](../s/standard_ExecutorStart.md)

## Notes and Other Information
- This is a static function only called from within execMain.c
- The function does not return a value; it either succeeds silently or throws an error
- In Hot Standby mode, temp tables cannot be created, so the temp table check is automatically satisfied
- The function is called early in query execution to fail fast on read-only violations
- Parallel mode has stricter rules than regular read-only mode due to the nature of parallel execution
- CTEs that modify data are treated as write operations even when part of a SELECT statement
- The function uses the namespace of relations to determine if they are temporary tables