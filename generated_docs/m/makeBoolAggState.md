# makeBoolAggState

## Location
src/backend/utils/adt/bool.c: 311 - 327

## Overview
Creates and initializes a new BoolAggState structure for boolean aggregation functions in the appropriate memory context.

## Definition


## Detailed Description
The makeBoolAggState function is a static helper function that allocates and initializes a new BoolAggState structure used by PostgreSQL's boolean aggregation functions (EVERY/ALL and SOME/ANY). The function ensures that it's called within an aggregate context and allocates the state structure in the aggregate's memory context to ensure proper lifetime management. The state is initialized with zero counts for both total non-null values (aggcount) and true values (aggtrue).

## Parameters / Member Variables
- `fcinfo`: FunctionCallInfo containing the function call context, used to verify aggregate context and obtain the appropriate memory context

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md)
  - [BoolAggState](../B/BoolAggState.md)
  - elog
- Called from (representative examples):
  - [bool_accum](../b/bool_accum.md)

## Notes and Other Information
This function is essential for the initialization phase of boolean aggregation operations. It validates that the function is being called in the correct aggregate context and will throw an error if called outside of an aggregation context. The allocated BoolAggState structure persists for the duration of the aggregation operation and is automatically cleaned up when the aggregate context is destroyed.