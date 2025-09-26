# RI_FKey_cascade_del

## Location
[src/backend/utils/adt/ri_triggers.c:743-848](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L743-L848)

## Overview
A trigger function that implements CASCADE behavior for DELETE operations, automatically deleting all rows in foreign key tables that reference the deleted primary key row.

## Definition
```c
Datum RI_FKey_cascade_del(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL trigger function that enforces the CASCADE referential integrity constraint when rows are deleted from a referenced (parent) table. When a CASCADE constraint is defined, deleting a row from the primary key table should automatically delete all rows in foreign key tables that reference the deleted row, maintaining referential integrity by eliminating orphaned references.

The function builds and executes a DELETE statement against the foreign key table to remove all rows that reference the deleted primary key values. The query constructed is of the form: `DELETE FROM [ONLY] <fktable> WHERE  = fkatt1 [AND ...]`, using the primary key values from the deleted row as parameters.

The function uses RowExclusiveLock on the foreign key relation since it will perform DELETE operations, and uses the SPI interface to execute the cascaded delete query.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `fcinfo`: Function call information structure containing trigger data and context

## Dependencies
- Functions called/Symbols referenced:
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md): Validates the trigger call context
  - [ri_FetchConstraintInfo](../r/ri_FetchConstraintInfo.md): Retrieves constraint metadata
  - `[table_open](../t/table_open.md)`: Opens the foreign key relation with RowExclusiveLock
  - [ri_BuildQueryKey](../r/ri_BuildQueryKey.md): Builds query cache key
  - [ri_FetchPreparedPlan](../r/ri_FetchPreparedPlan.md): Retrieves cached query plan
  - [ri_GenerateQual](../r/ri_GenerateQual.md): Generates WHERE clause conditions
  - [ri_PlanCheck](../r/ri_PlanCheck.md): Prepares and caches the DELETE query plan
  - [ri_PerformCheck](../r/ri_PerformCheck.md): Executes the cascaded delete operation
  - SPI functions: `SPI_connect`, `SPI_finish`
  - Various utility functions for name quoting and type handling
  - `RI_PLAN_CASCADE_ONDELETE`: Query plan type constant
  - `RI_TRIGTYPE_DELETE`: DELETE trigger type constant

- Called from (representative examples):
  - No direct callers found (invoked by PostgreSQL trigger system)

## Notes and Other Information
- This function is registered as a trigger function in the PostgreSQL system catalog
- Uses RowExclusiveLock mode on the foreign key relation since DELETE operations will be performed
- Implements query plan caching for performance optimization using `RI_PLAN_CASCADE_ONDELETE`
- Handles partitioned tables by omitting ONLY keyword when appropriate
- The cascaded deletes can trigger additional cascades if the foreign key tables have their own CASCADE constraints
- Part of PostgreSQL's comprehensive referential integrity system
- Located in `src/backend/utils/adt/ri_triggers.c` at lines 743-848
- Returns a Datum value as required by PostgreSQL's function call interface
- Uses `SPI_OK_DELETE` as the expected result from the delete operation