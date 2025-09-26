# ExecGetJunkAttribute

## Location
src/include/executor/executor.h: 190 - 244

## Overview
ExecGetJunkAttribute is a static inline function that retrieves attribute values from "junk" attributes in a TupleTableSlot, which are hidden system attributes not part of the regular tuple structure.

## Definition


## Detailed Description
ExecGetJunkAttribute provides a convenient interface for accessing junk attributes in tuple slots. Junk attributes are system-generated attributes that are not part of the user-visible tuple structure but are necessary for internal operations like row identification, system columns, or intermediate computation results. The function acts as a thin wrapper around slot_getattr, adding an assertion to ensure the attribute number is valid (greater than 0) and providing semantic clarity that this access is for junk attributes specifically.

This function is commonly used in execution contexts where the executor needs to access system-generated or hidden attributes that were added to tuples during query processing but are not part of the final result set.

## Parameters / Member Variables
- : TupleTableSlot containing the tuple from which to extract the junk attribute
- : AttrNumber specifying which junk attribute to retrieve (must be > 0)
- : Pointer to bool that will be set to indicate whether the retrieved attribute value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - slot_getattr (the underlying slot access function)
- Called from (representative examples):
  - EvalPlanQualFetchRowMark (for fetching row marks during EPQ processing)
  - ExecLockRows (for accessing row identification attributes)
  - ExecMergeMatched (for MERGE statement processing)
  - ExecModifyTable (for various modification operations)

## Notes and Other Information
- This is a static inline function defined in executor.h, making it efficiently accessible across the executor subsystem
- The Assert(attno > 0) ensures that only positive attribute numbers are accessed, preventing access to invalid attribute positions
- Junk attributes typically include system columns like ctid, tableoid, or intermediate values needed for query execution but not returned to the client
- The function maintains the same return semantics as slot_getattr, returning a Datum value and setting the isNull flag appropriately