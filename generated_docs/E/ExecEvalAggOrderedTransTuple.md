# ExecEvalAggOrderedTransTuple

## Location
[src/backend/executor/execExprInterp.c:5223-5236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L5223-L5236)

## Overview
This function invokes an ordered transition function for aggregate operations, specifically handling tuple arguments by preparing and storing them in a tuple sort state for ordered processing.

## Definition
```c
void ExecEvalAggOrderedTransTuple(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```

## Detailed Description
ExecEvalAggOrderedTransTuple is the tuple-based counterpart to ExecEvalAggOrderedTransDatum in PostgreSQL's expression evaluation framework for ordered aggregates. This function handles cases where aggregate functions need to process tuple (multi-column) arguments in a specific order. It prepares a tuple slot by clearing it, setting the number of valid attributes to match the number of inputs, storing it as a virtual tuple, and then feeding it to the tuple sort mechanism. This allows for proper ordering of complex data structures during aggregate processing.

## Parameters / Member Variables
- `state`: ExprState pointer containing the expression evaluation state  
- `op`: ExprEvalStep pointer containing the operation details, including the pertrans structure and set number
- `econtext`: ExprContext pointer providing the evaluation context (unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - ExecClearTuple
  - [ExecStoreVirtualTuple](ExecStoreVirtualTuple.md)  
  - [tuplesort_puttupleslot](../t/tuplesort_puttupleslot.md)
  - ExprEvalStep (struct)
  - [AggStatePerTrans](../A/AggStatePerTrans.md) (struct)
  - pg_attribute_always_inline
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- This function is specifically designed for ordered aggregates with tuple arguments
- The function manipulates the sortslot within the pertrans structure to prepare tuples for sorting
- Sets tts_nvalid to pertrans->numInputs to indicate how many attributes are valid in the tuple
- Uses ExecStoreVirtualTuple to efficiently store the tuple without copying data
- Works in conjunction with ExecEvalAggOrderedTransDatum for datum-based ordered aggregates
- Part of PostgreSQL's expression evaluation step execution framework
- The tuple sort state is accessed through pertrans->sortstates[setno] array