# FindTriggerIncompatibleWithInheritance

## Location
[src/backend/commands/trigger.c:2272-2303](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L2272-L2303)

## Overview
Checks if there is a row-level trigger with transition tables that prevents a table from becoming an inheritance child or partition, returning the name of the first incompatible trigger found.

## Definition
```c
const char *FindTriggerIncompatibleWithInheritance(TriggerDesc *trigdesc)
```

## Detailed Description
This function validates whether a table can be safely made into an inheritance child or partition by examining its triggers. It specifically looks for row-level triggers that use transition tables (OLD TABLE or NEW TABLE references), which are incompatible with inheritance relationships in PostgreSQL. When such triggers exist, they prevent the table from participating in inheritance hierarchies because transition tables cannot be properly maintained across inheritance boundaries.

The function iterates through all triggers in the provided TriggerDesc, checking each one for:
1. Row-level trigger type (statement-level triggers are allowed)
2. Usage of old table references (tgoldtable)
3. Usage of new table references (tgnewtable)

If any row-level trigger uses transition tables, the function immediately returns the name of that trigger, allowing the caller to provide a meaningful error message.

## Parameters / Member Variables
- `trigdesc`: Pointer to TriggerDesc structure containing all triggers defined on the table; can be NULL if no triggers exist

## Dependencies
- Functions called/Symbols referenced:
  - TRIGGER_FOR_ROW (macro to check if trigger is row-level)
- Data structures used:
  - [TriggerDesc](../T/TriggerDesc.md) (trigger descriptor structure)
  - [Trigger](../T/Trigger.md) (individual trigger structure)
- Called from (representative examples):
  - [ATExecAddInherit](../A/ATExecAddInherit.md) (when adding inheritance relationship)
  - [ATExecAttachPartition](../A/ATExecAttachPartition.md) (when attaching a partition)

## Notes and Other Information
- Returns NULL if no incompatible triggers are found, allowing inheritance to proceed
- Only row-level triggers are checked; statement-level triggers don't use transition tables
- The function stops at the first incompatible trigger found, prioritizing early detection
- This check is essential for maintaining PostgreSQL's inheritance semantics and preventing runtime errors
- Transition tables (OLD TABLE/NEW TABLE) are a PostgreSQL extension to the SQL standard for triggers