# array_subscript_assign_slice

## Location
[src/backend/utils/adt/arraysubs.c:344-398](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arraysubs.c#L344-L398)

## Overview
Handles array slice assignment operations for PostgreSQL's subscripting framework, evaluating assignments to array slices and updating the original array with replacement values.

## Definition

```c
struct_empty_array(workspace->refelemtype));
```
## Detailed Description
This function is responsible for executing array slice assignment operations within PostgreSQL's expression evaluation framework. It handles the assignment of replacement values to specified slices of arrays, dealing with both fixed-length and variable-length arrays. The function manages NULL handling appropriately - for fixed-length arrays, both the original array and replacement value must be non-NULL, while for variable-length arrays, a NULL original array is substituted with an empty array. The core assignment logic delegates to  to perform the actual array modification.

## Parameters / Member Variables
- : ExprState pointer containing expression evaluation state information
- : ExprEvalStep pointer containing the operation details and workspace data
- : ExprContext pointer providing the expression evaluation context

## Dependencies
- Functions called/Symbols referenced:
  - [ExprEvalStep](../E/ExprEvalStep.md) (struct)
  - [SubscriptingRefState](../S/SubscriptingRefState.md) (struct)
  - [ArraySubWorkspace](../A/ArraySubWorkspace.md) (struct)  
  - [construct_empty_array](../c/construct_empty_array.md)
  - [array_set_slice](array_set_slice.md)
- Called from (representative examples):
  - [array_exec_setup](array_exec_setup.md)

## Notes and Other Information
- This is a static function used internally within the array subscripting implementation
- Handles NULL array sources by constructing empty arrays for varlena types
- For fixed-length arrays, requires both source array and replacement value to be non-NULL
- The result is guaranteed to be non-NULL after successful execution
- Part of PostgreSQL's subscripting reference framework for array operations

## Simplified Source

```c
static void
array_subscript_assign_slice(ExprState *state,
                             ExprEvalStep *op,
                             ExprContext *econtext)
{
    SubscriptingRefState *sbsrefstate = op->d.sbsref.state;
    ArraySubWorkspace *workspace = (ArraySubWorkspace *) sbsrefstate->workspace;
    Datum arraySource = *op->resvalue;

    // Handle fixed-length arrays: both array and replacement must be non-NULL
    if (workspace->refattrlength > 0) {
        if (*op->resnull || sbsrefstate->replacenull)
            return;  // Keep original array unchanged
    }

    // Handle NULL original array for variable-length arrays
    if (*op->resnull) {
        // Create empty array to assign slice into
        arraySource = PointerGetDatum(construct_empty_array(workspace->refelemtype));
        *op->resnull = false;
    }

    // Perform the actual slice assignment
    *op->resvalue = array_set_slice(arraySource,
                                    sbsrefstate->numupper,
                                    workspace->upperindex,
                                    workspace->lowerindex,
                                    sbsrefstate->upperprovided,
                                    sbsrefstate->lowerprovided,
                                    sbsrefstate->replacevalue,
                                    sbsrefstate->replacenull,
                                    workspace->refattrlength,
                                    workspace->refelemlength,
                                    workspace->refelembyval,
                                    workspace->refelemalign);

    // Result is never NULL
}
```