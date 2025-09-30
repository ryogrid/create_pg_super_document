# ExecEvalRow

## Location
[src/backend/executor/execExprInterp.c:3100-3119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L3100-L3119)

## Overview
Evaluates a ROW() expression by constructing a tuple from individual column values that have been previously evaluated and stored.

## Definition
```c
void ExecEvalRow(ExprState *state, ExprEvalStep *op)
```

## Detailed Description
ExecEvalRow is an expression evaluation function that handles ROW() expressions in PostgreSQL's expression evaluation system. A ROW() expression creates a composite value (tuple) from a list of individual expressions. This function operates under the assumption that all individual column expressions have already been evaluated and their results stored in the operation's data structure.

The function uses heap_form_tuple to construct a proper HeapTuple from the pre-evaluated column values and null flags, then converts this tuple to a Datum for storage in the result. The result is never NULL at the tuple level (though individual fields within the tuple may be NULL).

## Parameters / Member Variables
- `state`: ExprState containing the overall expression evaluation context (currently unused in this function)
- `op`: ExprEvalStep containing the specific operation data including:
  - `op->d.row.tupdesc`: TupleDesc describing the structure of the result tuple
  - `op->d.row.elemvalues`: Array of Datum values for each column
  - `op->d.row.elemnulls`: Array of boolean flags indicating NULL status for each column
  - `op->resvalue`: Pointer to store the resulting tuple Datum
  - `op->resnull`: Pointer to NULL flag for the result (always set to false)

## Dependencies
- Functions called/Symbols referenced:
  - [heap_form_tuple](../h/heap_form_tuple.md): Creates a HeapTuple from column values and null flags
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md): Converts HeapTuple to Datum representation
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md): Main expression interpreter dispatch function
  - [FunctionReturningBool](../F/FunctionReturningBool.md): JIT compilation context

## Notes and Other Information
- This function assumes that individual column evaluations have been completed in prior evaluation steps
- The resulting tuple is always non-NULL, even if all individual fields are NULL
- Memory management for the created tuple follows PostgreSQL's memory context system
- Part of PostgreSQL's compiled expression evaluation framework for efficient row construction
- The function does not take an ExprContext parameter since all necessary evaluation has been completed previously

## Simplified Source

```c
void ExecEvalRow(ExprState *state, ExprEvalStep *op)
{
    // Build tuple from pre-evaluated column values
    HeapTuple tuple = heap_form_tuple(op->d.row.tupdesc,
                                      op->d.row.elemvalues,
                                      op->d.row.elemnulls);

    // Store tuple as result datum
    *op->resvalue = HeapTupleGetDatum(tuple);
    *op->resnull = false;  // ROW result is never NULL
}
```