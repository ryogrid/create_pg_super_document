# RI_FKey_setdefault_del

## Location
[src/backend/utils/adt/ri_triggers.c:1000-1014](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1000-L1014)

## Overview
This function implements a PostgreSQL referential integrity trigger that sets foreign key column values to their default values when the referenced primary key record is deleted.

## Definition

```c
Datum
RI_FKey_setdefault_del(PG_FUNCTION_ARGS)
```
## Detailed Description
RI_FKey_setdefault_del is a trigger function that enforces referential integrity by implementing the SET DEFAULT action for foreign key constraints on DELETE operations. When a record containing a primary key is deleted, this trigger is invoked on the foreign key table to set all matching foreign key values to their column default values, preventing orphaned references. This function serves as a wrapper that validates the trigger context and delegates the actual work to the shared ri_set function.

## Parameters / Member Variables
- : PostgreSQL function calling convention macro that provides access to function arguments and context through fcinfo

## Dependencies
- Functions called/Symbols referenced:
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md) (validates trigger call context)
  - [ri_set](../r/ri_set.md) (performs the actual SET DEFAULT operation)
  - RI_TRIGTYPE_DELETE (trigger type constant)
  - [TriggerData](../T/TriggerData.md) (structure containing trigger context information)
- Called from (representative examples):
  - No direct callers found (invoked by PostgreSQL trigger system)

## Notes and Other Information
- This function is designed to be used as a PostgreSQL trigger function
- It shares implementation code with RI_FKey_setdefault_upd through the ri_set function
- The function performs validation to ensure it's called in the correct trigger context (DELETE event)
- The second parameter to ri_set is false, indicating default values should be used instead of NULL
- Located in src/backend/utils/adt/ri_triggers.c:1000-1014