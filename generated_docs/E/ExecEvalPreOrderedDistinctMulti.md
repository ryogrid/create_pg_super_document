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

## Simplified Source

```c
bool ExecEvalPreOrderedDistinctMulti(AggState *aggstate, AggStatePerTrans pertrans)
{
    ExprContext *tmpcontext = aggstate->tmpcontext;
    bool is_distinct = false;

    // Copy current input values into sort slot
    for (int i = 0; i < pertrans->numTransInputs; i++) {
        pertrans->sortslot->tts_values[i] = pertrans->transfn_fcinfo->args[i + 1].value;
        pertrans->sortslot->tts_isnull[i] = pertrans->transfn_fcinfo->args[i + 1].isnull;
    }

    // Prepare current tuple for comparison
    ExecClearTuple(pertrans->sortslot);
    pertrans->sortslot->tts_nvalid = pertrans->numInputs;
    ExecStoreVirtualTuple(pertrans->sortslot);

    // Save and switch context slots for comparison
    TupleTableSlot *save_outer = tmpcontext->ecxt_outertuple;
    TupleTableSlot *save_inner = tmpcontext->ecxt_innertuple;
    tmpcontext->ecxt_outertuple = pertrans->sortslot;
    tmpcontext->ecxt_innertuple = pertrans->uniqslot;

    // Check if current values are different from previous
    if (!pertrans->haslast || !ExecQual(pertrans->equalfnMulti, tmpcontext)) {
        // Values are distinct - update the unique slot
        if (pertrans->haslast)
            ExecClearTuple(pertrans->uniqslot);

        pertrans->haslast = true;
        ExecCopySlot(pertrans->uniqslot, pertrans->sortslot);
        is_distinct = true;
    }

    // Restore original context slots
    tmpcontext->ecxt_outertuple = save_outer;
    tmpcontext->ecxt_innertuple = save_inner;

    return is_distinct;
}
```