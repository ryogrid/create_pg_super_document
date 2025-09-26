# ExecEvalPreOrderedDistinctMulti

## Location
[src/backend/executor/execExprInterp.c:5162-5208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L5162-L5208)

## Overview
ExecEvalPreOrderedDistinctMulti determines whether the current multi-argument aggregate input values are distinct from the previous input values for DISTINCT aggregates.

## Definition
bool ExecEvalPreOrderedDistinctMulti(AggState *aggstate, AggStatePerTrans pertrans)

## Detailed Description
This function implements the DISTINCT filtering logic for multi-argument aggregate functions by comparing the current set of input values with the previously processed set of values. It returns true when the current inputs are distinct from the previous inputs, indicating that the aggregate function should process these values. The function uses tuple slots to efficiently manage and compare multiple values simultaneously, leveraging the equality expression evaluation framework. It temporarily switches the expression context slots to perform the comparison and then restores the original context state.

## Parameters / Member Variables
- aggstate: AggState pointer containing the overall aggregation state including temporary context
- pertrans: AggStatePerTrans pointer containing per-transition state including sort slot, unique slot, and multi-argument equality function

## Dependencies
- Functions called/Symbols referenced:
  - [ExecClearTuple](ExecClearTuple.md)
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md)  
  - [ExecQual](ExecQual.md)
  - [ExecCopySlot](ExecCopySlot.md)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (in JIT compilation context)

## Notes and Other Information
- Part of the DISTINCT aggregate optimization system for pre-sorted input data with multiple arguments
- Uses tuple slots (sortslot and uniqslot) to efficiently manage multi-value comparisons
- Temporarily modifies the expression context outer and inner tuple slots for evaluation
- Carefully preserves and restores the original expression context state after comparison
- Optimized for cases where input data is already sorted, allowing efficient multi-argument DISTINCT processing
- Works with the equalfnMulti expression for performing multi-column equality checks
- Located in src/backend/executor/execExprInterp.c at lines 5162-5208