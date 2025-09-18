# RI_FKey_setdefault_upd

## Location
[src/backend/utils/adt/ri_triggers.c:1015-1030](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1015-L1030)

## Overview
This function implements a PostgreSQL referential integrity trigger that sets foreign key column values to their default values when the referenced primary key record is updated.

## Definition


## Detailed Description
RI_FKey_setdefault_upd is a trigger function that enforces referential integrity by implementing the SET DEFAULT action for foreign key constraints on UPDATE operations. When a primary key value is updated in the referenced table, this trigger is invoked on the foreign key table to set all matching foreign key values to their column default values, preventing orphaned references. This function serves as a wrapper that validates the trigger context and delegates the actual work to the shared ri_set function.

## Parameters / Member Variables
- : PostgreSQL function calling convention macro that provides access to function arguments and context through fcinfo

## Dependencies
- Functions called/Symbols referenced:
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md) (validates trigger call context)
  - [ri_set](../r/ri_set.md) (performs the actual SET DEFAULT operation)
  - RI_TRIGTYPE_UPDATE (trigger type constant)
  - TriggerData (structure containing trigger context information)
- Called from (representative examples):
  - No direct callers found (invoked by PostgreSQL trigger system)

## Notes and Other Information
- This function is designed to be used as a PostgreSQL trigger function
- It shares implementation code with RI_FKey_setdefault_del through the ri_set function
- The function performs validation to ensure it's called in the correct trigger context (UPDATE event)
- The second parameter to ri_set is false, indicating default values should be used instead of NULL
- Located in src/backend/utils/adt/ri_triggers.c:1015-1030