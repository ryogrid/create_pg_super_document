# ExecGetJunkAttribute

## Location
[src/include/executor/executor.h:190-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/executor.h#L190-L244)

## Overview
ExecGetJunkAttribute is a static inline function that retrieves attribute values from "junk" attributes in a TupleTableSlot, which are hidden system attributes not part of the regular tuple structure.

## Definition

```c
static inline Datum
ExecGetJunkAttribute(TupleTableSlot *slot, AttrNumber attno, bool *isNull)
```
## Detailed Description
ExecGetJunkAttribute provides a convenient interface for accessing junk attributes in tuple slots. Junk attributes are system-generated attributes that are not part of the user-visible tuple structure but are necessary for internal operations like row identification, system columns, or intermediate computation results. The function acts as a thin wrapper around slot_getattr, adding an assertion to ensure the attribute number is valid (greater than 0) and providing semantic clarity that this access is for junk attributes specifically.

This function is commonly used in execution contexts where the executor needs to access system-generated or hidden attributes that were added to tuples during query processing but are not part of the final result set.

## Parameters / Member Variables
- : TupleTableSlot containing the tuple from which to extract the junk attribute
- : AttrNumber specifying which junk attribute to retrieve (must be > 0)
- : Pointer to bool that will be set to indicate whether the retrieved attribute value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [slot_getattr](../s/slot_getattr.md) (the underlying slot access function)
- Called from (representative examples):
  - [EvalPlanQualFetchRowMark](EvalPlanQualFetchRowMark.md) (for fetching row marks during EPQ processing)
  - [ExecLockRows](ExecLockRows.md) (for accessing row identification attributes)
  - [ExecMergeMatched](ExecMergeMatched.md) (for MERGE statement processing)
  - [ExecModifyTable](ExecModifyTable.md) (for various modification operations)

## Notes and Other Information
- This is a static inline function defined in executor.h, making it efficiently accessible across the executor subsystem
- The Assert(attno > 0) ensures that only positive attribute numbers are accessed, preventing access to invalid attribute positions
- Junk attributes typically include system columns like ctid, tableoid, or intermediate values needed for query execution but not returned to the client
- The function maintains the same return semantics as slot_getattr, returning a Datum value and setting the isNull flag appropriately

## Simplified Source

```c
static inline Datum
ExecGetJunkAttribute(TupleTableSlot *slot, AttrNumber attno, bool *isNull)
{
    // Ensure valid attribute number
    Assert(attno > 0);

    // Get attribute value from slot
    return slot_getattr(slot, attno, isNull);
}
```