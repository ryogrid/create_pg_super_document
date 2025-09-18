# RI_FKey_restrict_upd

## Location
src/backend/utils/adt/ri_triggers.c: 608 - 623

## Overview
A trigger function that implements the RESTRICT referential integrity constraint behavior for UPDATE operations on the referenced table, preventing updates that would create orphaned foreign key references.

## Definition
```c
Datum RI_FKey_restrict_upd(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL trigger function that enforces the RESTRICT referential integrity constraint when rows in a referenced (parent) table are updated. The RESTRICT constraint type prevents any update to a primary key that is still referenced by foreign keys in other tables.

According to the SQL standard, RESTRICT should occur exactly when the update is performed, which is the conceptual difference from NO ACTION (which can be deferred). However, in PostgreSQL's implementation, both are implemented as non-deferrable AFTER triggers, making them functionally equivalent.

The function performs validation to ensure it's called in the correct trigger context and then delegates to the shared `ri_restrict` function for the actual constraint checking logic.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `fcinfo`: Function call information structure containing trigger data and context

## Dependencies
- Functions called/Symbols referenced:
  - `[ri_CheckTrigger](../r/ri_CheckTrigger.md)`: Validates the trigger call context
  - `[ri_restrict](../r/ri_restrict.md)`: Shared implementation for restriction-based constraints
  - `RI_TRIGTYPE_UPDATE`: Constant defining UPDATE trigger type
  - `TriggerData`: Structure containing trigger execution context
  
- Called from (representative examples):
  - No direct callers found (invoked by PostgreSQL trigger system)

## Notes and Other Information
- This function is registered as a trigger function in the PostgreSQL system catalog
- Despite the SQL standard difference between NO ACTION and RESTRICT, PostgreSQL implements both identically as non-deferrable AFTER triggers
- The function passes `false` as the second parameter to `ri_restrict`, distinguishing it from the NO ACTION case
- Part of the referential integrity (RI) trigger system that maintains foreign key constraints
- Located in `src/backend/utils/adt/ri_triggers.c` at lines 608-623
- Returns a Datum value as required by PostgreSQL's function call interface