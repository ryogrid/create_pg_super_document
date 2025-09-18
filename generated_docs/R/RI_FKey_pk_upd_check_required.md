# RI_FKey_pk_upd_check_required

## Location
[src/backend/utils/adt/ri_triggers.c:1226-1257](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1226-L1257)

## Overview
Determines whether a referential integrity trigger needs to be fired for a primary key update or delete operation by checking if the constraint could be violated.

## Definition
```c
bool RI_FKey_pk_upd_check_required(Trigger *trigger, Relation pk_rel,
                                   TupleTableSlot *oldslot, TupleTableSlot *newslot)
```

## Detailed Description
This function is called by the AFTER trigger queue manager to optimize referential integrity checking by determining whether an RI trigger actually needs to be fired for a primary key update or delete. It performs early checks to see if the foreign key constraint will definitely remain satisfied, allowing the system to skip unnecessary trigger execution.

The function implements several optimization strategies:
1. If any old primary key value is NULL, no foreign key could reference this row
2. If all old and new key values are identical (for updates), the constraint remains satisfied
3. Only when these conditions fail does it return true, indicating the trigger must fire

## Parameters / Member Variables
- `trigger`: The referential integrity trigger being considered for execution
- `pk_rel`: The primary key relation being updated or deleted from
- `oldslot`: TupleTableSlot containing the old tuple values
- `newslot`: TupleTableSlot containing the new tuple values (NULL for delete operations)

## Dependencies
- Functions called/Symbols referenced:
  - [ri_FetchConstraintInfo](../r/ri_FetchConstraintInfo.md)
  - [ri_NullCheck](../r/ri_NullCheck.md)
  - [ri_KeysEqual](../r/ri_KeysEqual.md)
  - RI_KEYS_NONE_NULL
- Called from (representative examples):
  - AfterTriggerSaveEvent

## Notes and Other Information
- This is a performance optimization function that helps reduce unnecessary trigger executions
- The function is located in src/backend/utils/adt/ri_triggers.c:1226-1257
- Returns false when the trigger can be safely skipped, true when it must be executed
- Part of PostgreSQL's referential integrity enforcement system