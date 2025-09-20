# RI_FKey_setnull_upd

## Location
[src/backend/utils/adt/ri_triggers.c:985-999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L985-L999)

## Overview
This function implements a PostgreSQL referential integrity trigger that sets foreign key column values to NULL when the referenced primary key record is updated.

## Definition

```c
Datum
RI_FKey_setnull_upd(PG_FUNCTION_ARGS)
```
## Detailed Description
RI_FKey_setnull_upd is a trigger function that enforces referential integrity by implementing the SET NULL action for foreign key constraints on UPDATE operations. When a primary key value is updated in the referenced table, this trigger is invoked on the foreign key table to set all matching foreign key values to NULL, preventing orphaned references. This function serves as a wrapper that validates the trigger context and delegates the actual work to the shared ri_set function.

## Parameters / Member Variables
- : PostgreSQL function calling convention macro that provides access to function arguments and context through fcinfo

## Dependencies
- Functions called/Symbols referenced:
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md) (validates trigger call context)
  - [ri_set](../r/ri_set.md) (performs the actual SET NULL operation)
  - RI_TRIGTYPE_UPDATE (trigger type constant)
  - TriggerData (structure containing trigger context information)
- Called from (representative examples):
  - No direct callers found (invoked by PostgreSQL trigger system)

## Notes and Other Information
- This function is designed to be used as a PostgreSQL trigger function
- It shares implementation code with RI_FKey_setnull_del through the ri_set function
- The function performs validation to ensure it's called in the correct trigger context (UPDATE event)
- Located in src/backend/utils/adt/ri_triggers.c:985-999