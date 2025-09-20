# ri_NullCheck

## Location
[src/backend/utils/adt/ri_triggers.c:2636-2672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L2636-L2672)

## Overview
Analyzes the NULL state of all key values in a tuple to determine if foreign key constraint checking should be performed.

## Definition

```c
static int
ri_NullCheck(TupleDesc tupDesc,
			 TupleTableSlot *slot,
			 const RI_ConstraintInfo *riinfo, bool rel_is_pk)
```
## Detailed Description
This function examines all key columns involved in a foreign key constraint to determine their collective NULL state. It returns one of three possible values indicating whether all keys are NULL, none are NULL, or some are NULL. This information is crucial for foreign key constraint enforcement, as foreign key semantics specify that if any part of a foreign key is NULL, the entire key is considered NULL and no constraint checking is required.

The function is essential for implementing proper foreign key semantics where partial NULL keys do not participate in referential integrity checks.

## Parameters / Member Variables
- : Tuple descriptor for the relation (currently unused in implementation)
- : Tuple slot containing the tuple to check for NULL values
- : Constraint information structure specifying which attributes to check
- : Boolean indicating whether to check primary key attributes (true) or foreign key attributes (false)

## Dependencies
- Functions called/Symbols referenced:
  - slot_attisnull
  - [RI_ConstraintInfo](../R/RI_ConstraintInfo.md) (structure access)
  - RI_KEYS_ALL_NULL (return constant)
  - RI_KEYS_NONE_NULL (return constant) 
  - RI_KEYS_SOME_NULL (return constant)
- Called from (representative examples):
  - [ri_Check_Pk_Match](ri_Check_Pk_Match.md)
  - [RI_FKey_pk_upd_check_required](../R/RI_FKey_pk_upd_check_required.md)
  - [RI_FKey_fk_upd_check_required](../R/RI_FKey_fk_upd_check_required.md)
  - [RI_Initial_Check](../R/RI_Initial_Check.md)

## Notes and Other Information
- Returns RI_KEYS_ALL_NULL when all key columns are NULL
- Returns RI_KEYS_NONE_NULL when no key columns are NULL  
- Returns RI_KEYS_SOME_NULL when some but not all key columns are NULL
- Used to implement SQL standard foreign key NULL semantics where any NULL in a composite foreign key makes the entire key NULL
- The tupDesc parameter is accepted but not used in the current implementation
- Critical for determining when foreign key constraint checks should be skipped