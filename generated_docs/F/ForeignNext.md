# ForeignNext

## Location
[src/backend/executor/nodeForeignscan.c:41-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeForeignscan.c#L41-L77)

## Overview
ForeignNext is a static workhorse function that executes the next iteration of a foreign scan operation, handling both SELECT queries and direct modification operations through the appropriate FDW routine.

## Definition

```c
static TupleTableSlot *
ForeignNext(ForeignScanState *node)
```
## Detailed Description
ForeignNext serves as the core execution engine for foreign scan operations in PostgreSQL's executor. It acts as an intermediary between the executor framework and Foreign Data Wrapper (FDW) routines, managing memory context switches and delegating the actual tuple retrieval to the appropriate FDW callback function.

The function operates in two modes:
1. For SELECT operations: Calls the FDW's IterateForeignScan routine to fetch the next tuple from the foreign data source
2. For direct modification operations (INSERT/UPDATE/DELETE): Calls the FDW's IterateDirectModify routine, with additional validation that these operations cannot occur during EvalPlanQual processing since direct modifications cannot be re-evaluated

Memory management is carefully handled by switching to the per-tuple memory context before calling FDW routines, ensuring proper cleanup of temporary allocations. The function also handles system column population by setting the tableoid when system columns are requested and a valid tuple is returned.

## Parameters / Member Variables
- : ForeignScanState structure containing the execution state for the foreign scan operation, including FDW routines, relation information, and execution context

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - IterateDirectModify (via fdwroutine)
  - IterateForeignScan (via fdwroutine)
  - TupIsNull
  - RelationGetRelid
- Called from:
  - [ExecForeignScan](../E/ExecForeignScan.md)

## Notes and Other Information
- This is a static function, only accessible within nodeForeignscan.c
- Direct modification operations include CMD_INSERT, CMD_UPDATE, and CMD_DELETE, which are distinguished from CMD_SELECT
- The function ensures proper memory context management to prevent memory leaks during FDW operations
- System column handling is conditional on plan->fsSystemCol flag and non-null tuple results
- Direct modifications are not compatible with EvalPlanQual processing due to their non-re-evaluatable nature