# slotNoNulls

## Location
[src/backend/executor/nodeSubplan.c:799-822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSubplan.c#L799-L822)

## Overview
Determines whether a TupleTableSlot contains no NULL values in any of its columns, used in subplan execution for complete tuple validation.

## Definition
```c
static bool slotNoNulls(TupleTableSlot *slot)
```

## Detailed Description
slotNoNulls performs a comprehensive check to verify that every attribute in a TupleTableSlot contains a non-NULL value. The function iterates through all columns in the slot's tuple descriptor and uses slot_attisnull() to test each attribute for NULL values, returning true only if all attributes are non-NULL. This is the logical complement to slotAllNulls and is specifically designed for projected tuples.

Like its counterpart slotAllNulls, this function intentionally does not handle dropped columns, which is acceptable for its intended use case. It's used in subplan execution contexts where ensuring complete, non-NULL tuples is important for proper query semantics and optimization decisions.

## Parameters / Member Variables
- `slot`: The TupleTableSlot to check for the absence of NULL values across all columns

## Dependencies
- Functions called/Symbols referenced:
  - slot_attisnull (to check individual attribute NULL status)
- Types used:
  - TupleTableSlot
  - [SubPlanState](../S/SubPlanState.md) (referenced in broader context)
- Called from (representative examples):
  - [ExecHashSubPlan](../E/ExecHashSubPlan.md) (for complete tuple validation in subplan execution)
  - [buildSubPlanHash](../b/buildSubPlanHash.md) (during hash table construction for subplans)

## Notes and Other Information
- This is a static function internal to nodeSubplan.c, used exclusively for subplan operations
- Does not test for dropped columns, which is acceptable since it's only used on projected tuples
- Uses 1-based attribute numbering (i = 1 to ncols) consistent with PostgreSQL's attribute indexing convention
- Returns false immediately upon finding the first NULL attribute, optimizing for early termination
- The function assumes the slot's tuple descriptor is valid and accessible
- Provides the inverse functionality of slotAllNulls, together forming a complete NULL status checking toolkit for subplan operations