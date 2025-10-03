# ExecEvalExpr

## Location
[src/include/executor/executor.h:334-348](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/executor.h#L334-L348)

## Overview
ExecEvalExpr is the primary interface function for evaluating SQL expressions in PostgreSQL, delegating to compiled expression evaluation functions for optimal performance.

## Definition

```c
static inline Datum
ExecEvalExpr(ExprState *state,
			 ExprContext *econtext,
			 bool *isNull)
```
## Detailed Description
ExecEvalExpr serves as the main entry point for expression evaluation in PostgreSQL's executor. It implements a function pointer dispatch mechanism where the actual evaluation is performed by a compiled evaluation function stored in the ExprState's evalfunc field. This design enables PostgreSQL's Just-In-Time (JIT) compilation system and step-wise expression compilation to optimize expression evaluation performance.

The function takes an expression state (containing the compiled evaluation function and other metadata), an execution context (providing access to tuple data and parameters), and returns the computed result along with a null indicator. This uniform interface allows expressions to be evaluated consistently across the entire query execution system.

## Parameters / Member Variables
- `*state`: ExprState containing the compiled expression evaluation function and associated metadata
- `*econtext`: ExprContext providing access to current tuple data, parameters, and evaluation context
- `*isNull`: Pointer to bool that will be set to indicate whether the evaluated result is NULL
## Dependencies
- Functions called/Symbols referenced:
  - [state](../s/state.md)->evalfunc (the compiled expression evaluation function)
- Called from (representative examples):
  - [StoreAttrDefault](../S/StoreAttrDefault.md) (for column default expressions)
  - [NextCopyFrom](../N/NextCopyFrom.md) (during COPY operations)
  - [ATRewriteTable](../A/ATRewriteTable.md) (during ALTER TABLE operations)
  - [ExecMakeTableFunctionResult](ExecMakeTableFunctionResult.md) (for table functions)
  - [finalize_aggregate](../f/finalize_aggregate.md) (in aggregate processing)
  - [ExecHashGetHashValue](ExecHashGetHashValue.md) (for hash computations)
  - Various expression evaluation contexts throughout the executor

## Notes and Other Information
- This is a static inline function defined in executor.h, ensuring efficient calls across the executor subsystem
- The evalfunc pointer enables PostgreSQL's expression compilation system, where expressions are compiled into optimized step-based evaluation sequences
- The function supports JIT compilation when available, allowing for native code generation of expression evaluation
- The ExprContext parameter provides access to inner/outer tuple slots, scan slots, parameters, and other evaluation context
- The uniform interface allows the same function to be used for WHERE clauses, SELECT targets, function arguments, and any other expression evaluation needs
- Performance is critical for this function as it's called for every expression evaluation in query execution

## Simplified Source

```c
static inline Datum ExecEvalExpr(ExprState *state,
                                ExprContext *econtext,
                                bool *isNull) {
    // Delegate to the compiled evaluation function
    return state->evalfunc(state, econtext, isNull);
}
```