# fetch_tuple_flag

## Location
[src/backend/executor/nodeSetOp.c:102-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeSetOp.c#L102-L119)

## Overview
Extracts the flag column value from an input tuple to determine whether the tuple originates from the left or right input relation in SetOp operations.

## Definition

```c
static int
fetch_tuple_flag(SetOpState *setopstate, TupleTableSlot *inputslot)
```
## Detailed Description
This function retrieves the special flag column from a tuple that indicates which input relation (left or right) produced the tuple. The flag column is an integer field that PostgreSQL's planner adds to tuples during SetOp operations to track their origin. The function performs strict validation to ensure the flag value is either 0 (left input) or 1 (right input) and that the column is not NULL.

The function accesses the flag column using the flagColIdx stored in the SetOp plan node, extracts the datum value using slot_getattr, converts it to an integer using DatumGetInt32, and validates its correctness with assertions. This flag information is crucial for implementing proper set operation semantics where the behavior depends on which input relation contributed each tuple.

## Parameters / Member Variables
- `*setopstate`: Pointer to the SetOpState execution state containing plan information and runtime state
- `*inputslot`: TupleTableSlot containing the input tuple from which to extract the flag value
## Dependencies
- Functions called/Symbols referenced:
  - [SetOpState](../S/SetOpState.md) (execution state structure)
  - [SetOp](../S/SetOp.md) (plan node structure)
  - [slot_getattr](../s/slot_getattr.md) (tuple slot access function)
  - [DatumGetInt32](../D/DatumGetInt32.md) (datum conversion function)
  - Assert (assertion macro)
- Called from (representative examples):
  - [setop_retrieve_direct](../s/setop_retrieve_direct.md) (multiple locations)
  - [setop_fill_hash_table](../s/setop_fill_hash_table.md)

## Notes and Other Information
- The flag column is added by PostgreSQL's planner and is not part of the original user data
- Validates that the flag value is exactly 0 or 1, ensuring data integrity
- Uses assertions to catch programming errors during development and debug builds
- Essential for distinguishing tuple sources in UNION, INTERSECT, and EXCEPT operations
- The flagColIdx is determined during plan creation and stored in the SetOp plan node

## Simplified Source

```c
static int
fetch_tuple_flag(SetOpState *setopstate, TupleTableSlot *inputslot)
{
    SetOp *node = (SetOp *) setopstate->ps.plan;
    int flag;
    bool isNull;

    // Extract flag column value (0 = left input, 1 = right input)
    flag = DatumGetInt32(slot_getattr(inputslot, node->flagColIdx, &isNull));

    // Validate the flag value
    Assert(!isNull);
    Assert(flag == 0 || flag == 1);

    return flag;
}
```