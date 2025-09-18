# ExecEvalPreOrderedDistinctSingle

## Location
[src/backend/executor/execExprInterp.c:5119-5161](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L5119-L5161)

## Overview
ExecEvalPreOrderedDistinctSingle determines whether the current aggregate input value is distinct from the previous input value for single-argument DISTINCT aggregates.

## Definition
bool ExecEvalPreOrderedDistinctSingle(AggState *aggstate, AggStatePerTrans pertrans)

## Detailed Description
This function implements the DISTINCT filtering logic for single-argument aggregate functions by comparing the current input value with the previously processed value. It returns true when the current input is distinct from the previous input, indicating that the aggregate function should process this value. The function maintains state about the last processed value and performs equality comparisons using the appropriate equality function. It handles both NULL and non-NULL values correctly, and manages memory for pass-by-reference data types by copying values into the aggregation context.

## Parameters / Member Variables
- aggstate: AggState pointer containing the overall aggregation state and memory contexts
- pertrans: AggStatePerTrans pointer containing per-transition state including last value cache and equality function information

## Dependencies
- Functions called/Symbols referenced:
  - [FunctionCall2Coll](../F/FunctionCall2Coll.md)
  - [datumCopy](../d/datumCopy.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [DatumGetBool](../D/DatumGetBool.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (in JIT compilation context)

## Notes and Other Information
- Part of the DISTINCT aggregate optimization system for pre-sorted input data
- Handles memory management for pass-by-reference data types by copying values when necessary
- Uses the configured equality function and collation for proper value comparisons
- Optimized for cases where input data is already sorted, allowing efficient DISTINCT processing
- Manages the haslast, lastdatum, and lastisnull fields in the pertrans structure for state tracking
- Located in src/backend/executor/execExprInterp.c at lines 5119-5161