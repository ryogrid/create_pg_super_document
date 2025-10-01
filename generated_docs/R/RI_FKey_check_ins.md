# RI_FKey_check_ins

## Location
[src/backend/utils/adt/ri_triggers.c:424-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L424-L439)

## Overview
Trigger function that validates foreign key constraints during INSERT operations on a foreign key table by checking that the referenced key exists in the primary key table.

## Definition

```c
Datum
RI_FKey_check_ins(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the entry point for foreign key constraint validation during INSERT operations. It performs initial validation to ensure it's called in the correct trigger context (INSERT event), then delegates the actual constraint checking to the shared `RI_FKey_check` function.

The function implements the PostgreSQL foreign key constraint mechanism by:
1. Validating that the trigger is properly configured for INSERT operations
2. Calling the core constraint validation logic that checks if the foreign key values exist in the referenced primary key table
3. Supporting all foreign key match types (SIMPLE, FULL) and handling NULL values according to SQL standards

The actual constraint validation includes checking for NULL key handling, building dynamic SQL queries to verify existence in the primary key table, and proper locking to ensure data consistency.

## Parameters / Member Variables
This function follows PostgreSQL's trigger function interface:
- Uses `PG_FUNCTION_ARGS` macro which provides access to `FunctionCallInfo fcinfo`
- `fcinfo->context` contains the `TriggerData` structure with trigger execution context

## Dependencies
- Functions called/Symbols referenced:
  - [ri_CheckTrigger](../r/ri_CheckTrigger.md)
  - RI_FKey_check
  - RI_TRIGTYPE_INSERT (constant)
  - [TriggerData](../T/TriggerData.md) (type cast)
- Called from (representative examples):
  - [validateForeignKeyConstraint](../v/validateForeignKeyConstraint.md) (src/backend/commands/tablecmds.c:12318)

## Notes and Other Information
- This function is typically installed as a trigger function on foreign key tables
- It shares core logic with `RI_FKey_check_upd` through the common `RI_FKey_check` function
- The function returns `Datum` following PostgreSQL's function call convention
- Part of PostgreSQL's referential integrity (RI) system located in src/backend/utils/adt/ri_triggers.c:424-431
- Handles constraint validation for all INSERT operations where foreign key constraints are defined

## Simplified Source

```c
Datum RI_FKey_check_ins(PG_FUNCTION_ARGS) {
    // Validate this is called from correct trigger context (INSERT)
    ri_CheckTrigger(fcinfo, "RI_FKey_check_ins", RI_TRIGTYPE_INSERT);

    // Delegate to shared foreign key validation logic
    return RI_FKey_check((TriggerData *) fcinfo->context);
}
```