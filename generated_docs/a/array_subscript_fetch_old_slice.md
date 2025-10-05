# array_subscript_fetch_old_slice

## Location
[src/backend/utils/adt/arraysubs.c:439-472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arraysubs.c#L439-L472)

## Overview
Retrieves the old array slice value during assignment expression evaluation, designed for cases where the new-value subexpression contains SubscriptingRef or FieldStore operations, though currently unreachable in practice.

## Definition
```c
static void array_subscript_fetch_old_slice(ExprState *state,
                                           ExprEvalStep *op,
                                           ExprContext *econtext)
```

## Detailed Description
This function is designed to fetch the existing slice value from an array before assignment operations, similar to `array_subscript_fetch_old` but for slice operations rather than individual elements. The function handles NULL arrays by setting the previous slice value as NULL. For non-NULL arrays, it delegates to `array_get_slice` to extract the current slice at the specified bounds. However, according to the source comments, this is currently dead code because slice assignments cannot directly contain FieldStore operations or nested SubscriptingRef assignments under current PostgreSQL semantics.

## Parameters / Member Variables
- `state`: ExprState pointer containing expression evaluation state information
- `op`: ExprEvalStep pointer containing the operation details and workspace data
- `econtext`: ExprContext pointer providing the expression evaluation context

## Dependencies
- Functions called/Symbols referenced:
  - [ExprEvalStep](../E/ExprEvalStep.md) (struct)
  - [SubscriptingRefState](../S/SubscriptingRefState.md) (struct)  
  - [ArraySubWorkspace](../A/ArraySubWorkspace.md) (struct)
  - [array_get_slice](array_get_slice.md)
- Called from (representative examples):
  - [array_exec_setup](array_exec_setup.md)

## Notes and Other Information
- This is currently dead code - not reachable under current PostgreSQL array operation semantics
- Designed for potential future generalizations that might make slice-based old value fetching necessary
- Handles NULL arrays by setting both prevvalue to 0 and prevnull to true
- For non-NULL arrays, slices are never considered NULL (prevnull is always set to false)
- Part of the comprehensive array subscripting framework but unused in practice
- Future PostgreSQL versions might activate this code path with enhanced array operation support

## Simplified Source

```c
static void
array_subscript_fetch_old_slice(ExprState *state,
                                ExprEvalStep *op,
                                ExprContext *econtext)
{
    SubscriptingRefState *sbsrefstate = op->d.sbsref.state;
    ArraySubWorkspace *workspace = (ArraySubWorkspace *) sbsrefstate->workspace;

    if (*op->resnull) {
        // Whole array is null, so any slice is too
        sbsrefstate->prevvalue = (Datum) 0;
        sbsrefstate->prevnull = true;
    } else {
        // Fetch the current slice value before assignment
        sbsrefstate->prevvalue = array_get_slice(*op->resvalue,
                                                 sbsrefstate->numupper,
                                                 workspace->upperindex,
                                                 workspace->lowerindex,
                                                 sbsrefstate->upperprovided,
                                                 sbsrefstate->lowerprovided,
                                                 workspace->refattrlength,
                                                 workspace->refelemlength,
                                                 workspace->refelembyval,
                                                 workspace->refelemalign);
        // Slices of non-null arrays are never null
        sbsrefstate->prevnull = false;
    }
}
```