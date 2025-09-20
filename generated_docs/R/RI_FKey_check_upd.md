# RI_FKey_check_upd

## Location
[src/backend/utils/adt/ri_triggers.c:440-460](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L440-L460)

## Overview
Trigger function that validates foreign key constraints during UPDATE operations on a foreign key table by checking that the updated referenced key values exist in the primary key table.

## Definition

```c
Datum
RI_FKey_check_upd(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the entry point for foreign key constraint validation during UPDATE operations on tables with foreign key constraints. It performs initial validation to ensure it's called in the correct trigger context (UPDATE event), then delegates the actual constraint checking to the shared `RI_FKey_check` function.

The function implements foreign key constraint validation for UPDATE operations by:
1. Validating that the trigger is properly configured for UPDATE operations using `RI_TRIGTYPE_UPDATE`
2. Calling the core constraint validation logic that verifies the updated foreign key values exist in the referenced primary key table
3. Supporting all foreign key match types (SIMPLE, FULL) and proper NULL value handling

The core validation logic handles building dynamic SQL queries, proper locking mechanisms, and ensures referential integrity is maintained after the update operation.

## Parameters / Member Variables
This function follows PostgreSQL's trigger function interface:
- Uses `PG_FUNCTION_ARGS` macro which provides access to `FunctionCallInfo fcinfo`
- `fcinfo->context` contains the `TriggerData` structure with trigger execution context including both old and new tuple data

## Dependencies
- Functions called/Symbols referenced:
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md)
  - RI_FKey_check
  - RI_TRIGTYPE_UPDATE (constant)
  - TriggerData (type cast)
- Called from (representative examples):
  - No direct references found in the codebase analysis

## Notes and Other Information
- This function is typically installed as a trigger function on foreign key tables for UPDATE events
- It shares core constraint validation logic with `RI_FKey_check_ins` through the common `RI_FKey_check` function
- The `RI_FKey_check` function can distinguish between INSERT and UPDATE contexts and handles the new tuple appropriately
- Located in src/backend/utils/adt/ri_triggers.c:440-447
- Part of PostgreSQL's comprehensive referential integrity system
- Only validates constraints if the foreign key columns are actually modified in the UPDATE operation