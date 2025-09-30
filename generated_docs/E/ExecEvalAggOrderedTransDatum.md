# ExecEvalAggOrderedTransDatum

## Location
[src/backend/executor/execExprInterp.c:5209-5222](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L5209-L5222)

## Overview
This function invokes an ordered transition function for aggregate operations, specifically handling datum (single value) arguments by storing them in a tuple sort state for ordered processing.

## Definition

```c
void
ExecEvalAggOrderedTransDatum(ExprState *state, ExprEvalStep *op,
							 ExprContext *econtext)
```
## Detailed Description
ExecEvalAggOrderedTransDatum is part of PostgreSQL's expression evaluation framework for aggregate functions that require ordered processing. When an aggregate function needs to process its input values in a specific order (such as string_agg with ORDER BY), this function handles the storage of individual datum values into a tuple sort structure. The function extracts the datum value and its null indicator from the operation step and feeds them to the tuple sort mechanism, which will later be used to retrieve the values in the correct order during the transition phase.

## Parameters / Member Variables
- `state`: ExprState pointer containing the expression evaluation state
- `op`: ExprEvalStep pointer containing the operation details, including the pertrans structure and set number
- `econtext`: ExprContext pointer providing the evaluation context (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [tuplesort_putdatum](../t/tuplesort_putdatum.md)
  - [ExprEvalStep](ExprEvalStep.md) (struct)
  - [AggStatePerTrans](../A/AggStatePerTrans.md) (struct)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- This function is specifically designed for ordered aggregates with datum arguments
- The function accesses the sort state through pertrans->sortstates[setno] array
- The datum value and null flag are accessed through op->resvalue and op->resnull
- This is part of the expression evaluation step execution in PostgreSQL's executor
- The function works in conjunction with ExecEvalAggOrderedTransTuple for tuple-based ordered aggregates

## Simplified Source

```c
void ExecEvalAggOrderedTransDatum(ExprState *state, ExprEvalStep *op,
                                 ExprContext *econtext)
{
    AggStatePerTrans pertrans = op->d.agg_trans.pertrans;
    int setno = op->d.agg_trans.setno;

    // Store datum value in tuple sort for ordered processing
    tuplesort_putdatum(pertrans->sortstates[setno],
                      *op->resvalue, *op->resnull);
}
```