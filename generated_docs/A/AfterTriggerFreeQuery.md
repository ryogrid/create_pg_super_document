# AfterTriggerFreeQuery

## Location
[src/backend/commands/trigger.c:5216-5283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L5216-L5283)

## Overview
Releases all subsidiary storage associated with a trigger query level, including event lists, tuplestores, and table-specific data structures.

## Definition
```c
static void AfterTriggerFreeQuery(AfterTriggersQueryData *qs)
```

## Detailed Description
AfterTriggerFreeQuery performs comprehensive cleanup of resources allocated for a specific trigger query level. The function systematically releases trigger events, FDW (Foreign Data Wrapper) tuplestores, and per-table subsidiary storage including old/new tuple storage for updates, deletes, and inserts.

The function is designed to be safe when interrupted by errors and called again for the same query level, implementing defensive programming practices by setting pointers to NULL before freeing resources and using careful ordering of cleanup operations.

## Parameters / Member Variables
- `qs`: Pointer to AfterTriggersQueryData structure containing query-level trigger state and resources to be released

## Dependencies
- Functions called/Symbols referenced:
  - [AfterTriggersQueryData](AfterTriggersQueryData.md) (query-level trigger state structure)
  - [Tuplestorestate](../T/Tuplestorestate.md) (tuple storage state structure)
  - [afterTriggerFreeEventList](../a/afterTriggerFreeEventList.md) (frees trigger event list)
  - [tuplestore_end](../t/tuplestore_end.md) (closes tuple storage)
  - [AfterTriggersTableData](AfterTriggersTableData.md) (per-table trigger data structure)
  - [ExecDropSingleTupleTableSlot](../E/ExecDropSingleTupleTableSlot.md) (releases tuple slot)
  - [list_free_deep](../l/list_free_deep.md) (deep frees list and contents)
- Called from:
  - [AfterTriggerEndQuery](AfterTriggerEndQuery.md) (in src/backend/commands/trigger.c:5202)
  - [AfterTriggerEndSubXact](AfterTriggerEndSubXact.md) (in src/backend/commands/trigger.c:5478)

## Notes and Other Information
- Designed for exception safety - can be safely called multiple times for the same query level
- Handles multiple types of tuplestores per table: old_upd, new_upd, old_del, new_ins
- Sets pointers to NULL before freeing to prevent double-free errors
- Uses defensive programming by resetting qs->tables to NIL before calling list_free_deep
- Properly manages TupleTableSlot cleanup through ExecDropSingleTupleTableSlot
- Part of PostgreSQL's resource management for deferred trigger execution
- Static function only called internally within the trigger subsystem