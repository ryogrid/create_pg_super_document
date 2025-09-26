# RI_FKey_fk_upd_check_required

## Location
[src/backend/utils/adt/ri_triggers.c:1258-1358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L1258-L1358)

## Overview
Determines whether a referential integrity trigger needs to be fired for a foreign key update operation by analyzing NULL values, match types, and key equality.

## Definition
```c
bool RI_FKey_fk_upd_check_required(Trigger *trigger, Relation fk_rel,
                                   TupleTableSlot *oldslot, TupleTableSlot *newslot)
```

## Detailed Description
This function optimizes referential integrity checking for foreign key updates by determining whether an RI trigger actually needs to be executed. It implements complex logic based on NULL handling rules and foreign key match types to avoid unnecessary constraint checks.

The function handles several scenarios:
1. **All NULL values**: If all new foreign key values are NULL, the constraint is satisfied
2. **Some NULL values**: Behavior depends on the match type:
   - MATCH SIMPLE: Any NULL satisfies the constraint
   - MATCH FULL: Some NULLs violate the constraint
   - MATCH PARTIAL: Must run full check
3. **Transaction context**: If the original row was inserted by the current transaction, the trigger must fire
4. **Key equality**: If old and new key values are identical, no check is needed

## Parameters / Member Variables
- `trigger`: The referential integrity trigger being considered for execution
- `fk_rel`: The foreign key relation being updated
- `oldslot`: TupleTableSlot containing the old tuple values
- `newslot`: TupleTableSlot containing the new tuple values

## Dependencies
- Functions called/Symbols referenced:
  - [ri_FetchConstraintInfo](../r/ri_FetchConstraintInfo.md)
  - [ri_NullCheck](../r/ri_NullCheck.md)
  - [slot_is_current_xact_tuple](../s/slot_is_current_xact_tuple.md)
  - [ri_KeysEqual](../r/ri_KeysEqual.md)
  - RI_KEYS_ALL_NULL
  - RI_KEYS_SOME_NULL
  - FKCONSTR_MATCH_SIMPLE
  - FKCONSTR_MATCH_PARTIAL
  - FKCONSTR_MATCH_FULL
- Called from (representative examples):
  - [AfterTriggerSaveEvent](../A/AfterTriggerSaveEvent.md)

## Notes and Other Information
- This function is never called for partitioned tables due to how AfterTriggerSaveEvent() handles them
- Located in src/backend/utils/adt/ri_triggers.c:1258-1358
- Implements sophisticated NULL handling logic according to SQL standard foreign key match types
- Part of PostgreSQL's referential integrity optimization system
- Returns false when the trigger can be safely skipped, true when it must be executed