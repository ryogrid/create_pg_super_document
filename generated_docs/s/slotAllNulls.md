# slotAllNulls

## Location
src/backend/executor/nodeSubplan.c: 779 - 798

## Overview
Determines whether a TupleTableSlot contains only NULL values in all of its columns, used in subplan execution for NULL tuple detection.

## Definition
```c
static bool slotAllNulls(TupleTableSlot *slot)
```

## Detailed Description
slotAllNulls performs a comprehensive check to determine if every attribute in a TupleTableSlot is NULL. The function iterates through all columns in the slot's tuple descriptor and uses slot_attisnull() to test each attribute for NULL values. This is specifically designed for projected tuples and intentionally does not handle dropped columns, as the comment indicates this limitation is acceptable for its intended use case.

The function is used in subplan execution contexts where detecting completely NULL tuples is important for proper query semantics, particularly in scenarios involving outer joins or other operations that can produce NULL-filled result tuples.

## Parameters / Member Variables
- `slot`: The TupleTableSlot to check for NULL values across all columns

## Dependencies
- Functions called/Symbols referenced:
  - slot_attisnull (to check individual attribute NULL status)
- Types used:
  - TupleTableSlot
- Called from (representative examples):
  - [ExecHashSubPlan](../E/ExecHashSubPlan.md) (for NULL tuple detection in subplan execution)

## Notes and Other Information
- This is a static function internal to nodeSubplan.c, used exclusively for subplan operations
- Does not test for dropped columns, which is acceptable since it's only used on projected tuples
- Uses 1-based attribute numbering (i = 1 to ncols) consistent with PostgreSQL's attribute indexing convention
- Returns false immediately upon finding the first non-NULL attribute, optimizing for early termination
- The function assumes the slot's tuple descriptor is valid and accessible