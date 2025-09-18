# advance_windowaggregate_base

## Location
src/backend/executor/nodeWindowAgg.c: 419 - 581

## Overview
Removes the oldest tuple from a window aggregate by calling the aggregate's inverse transition function, supporting efficient sliding window computations.

## Definition


## Detailed Description
This function is the counterpart to  and handles the removal of tuples from moving window aggregates. It calls the inverse transition function to efficiently remove the contribution of the oldest tuple from the aggregate's current state. The function includes several safety mechanisms: it validates that the aggregate state is not NULL (as required for moving aggregates), handles the special case of removing the last tuple by reinitializing the aggregate, and returns false if the inverse transition function indicates it cannot perform the removal (forcing a restart). Like its forward counterpart, it handles filtering, strict functions, and careful memory management.

## Parameters / Member Variables
- : The overall window aggregate execution state containing temporary contexts and current aggregate context
- : Per-function state containing the window function expression state, number of arguments, and collation information  
- : Per-aggregate state containing inverse transition function info, current transition values, and memory context

## Dependencies
- Functions called/Symbols referenced:
  - ExecEvalExpr
  - [initialize_windowaggregate](../i/initialize_windowaggregate.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - DatumIsReadWriteExpandedObject
  - DatumGetEOHP
  - MemoryContextGetParent
  - [datumCopy](../d/datumCopy.md)
  - [DeleteExpandedObject](../D/DeleteExpandedObject.md)
- Called from (representative examples):
  - [eval_windowaggregates](../e/eval_windowaggregates.md)

## Notes and Other Information
- Returns  if successful,  if the inverse transition function cannot handle the removal (forcing aggregate restart)
- Handles FILTER clauses by evaluating the filter expression and returning success immediately for filtered-out tuples
- For strict inverse transition functions, skips processing when any argument is NULL and returns success
- When  reaches 1, reinitializes the aggregate instead of calling the inverse function to ensure proper initial state
- Enforces that the transition state must not be NULL in moving-aggregate mode (throws error if violated)
- Decrements  to track the number of tuples contributing to the current state
- Same sophisticated memory management as  including expanded object detection
- Sets  during inverse transition function calls to support AggCheckCallContext