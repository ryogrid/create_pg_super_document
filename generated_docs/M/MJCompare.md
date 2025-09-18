# MJCompare

## Location
src/backend/executor/nodeMergejoin.c: 391 - 451

## Overview
Compares the mergejoinable values of the current outer and inner tuples, returning an integer indicating their relative ordering for merge join processing.

## Definition


## Detailed Description
This function performs the core comparison logic for merge joins by comparing the evaluated expressions from the current outer and inner tuples. The function:

1. **Sequential comparison**: Iterates through all merge join clauses in order, comparing corresponding left (outer) and right (inner) expressions
2. **NULL handling**: Treats NULL-vs-NULL comparisons as a special case, considering them equal for comparison purposes but marking this condition to prevent tuple matching
3. **Short-circuit evaluation**: Stops comparing as soon as a non-equal result is found in any clause
4. **Sort-aware comparison**: Uses ApplySortComparator with the pre-configured sort support data to perform efficient comparisons
5. **Equality prevention**: Ensures tuples with NULL-vs-NULL comparisons or constant-false join conditions are not considered equal by returning +1 instead of 0

The function assumes that MJEvalOuterValues and MJEvalInnerValues have already been called to populate the datum values and null flags.

## Parameters / Member Variables
- : The MergeJoinState containing the evaluated expression values, null flags, and merge clauses

## Dependencies
- Functions called/Symbols referenced:
  - ResetExprContext
  - MemoryContextSwitchTo
  - ApplySortComparator
- Called from:
  - ExecMergeJoin (multiple call sites)

## Notes and Other Information
- Returns 0 if outer == inner (all merge conditions succeed), >0 if outer > inner, <0 if outer < inner
- Uses short-lived expression context to prevent memory leaks from comparison functions
- The special handling of NULL-vs-NULL ensures that such tuples advance the inner side rather than being considered matches
- Handles constant-false join conditions by preventing equality even when merge keys are equal
- Critical for merge join correctness as it determines the tuple advancement strategy during the join process
- Memory context switching ensures proper cleanup of any allocations made during comparison