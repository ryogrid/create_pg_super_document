# ExecAggPlainTransByVal

## Location
src/backend/executor/execExprInterp.c: 5237 - 5268

## Overview  
This function implements the transition function invocation for aggregate operations on pass-by-value data types, handling the core logic of updating aggregate state values.

## Definition
```c
static pg_attribute_always_inline void ExecAggPlainTransByVal(AggState *aggstate, AggStatePerTrans pertrans, AggStatePerGroup pergroup, ExprContext *aggcontext, int setno)
```

## Detailed Description
ExecAggPlainTransByVal is a critical function in PostgreSQL's aggregate execution framework that handles transition function calls for pass-by-value data types. When processing aggregate functions like SUM, COUNT, or AVG, this function manages the invocation of the aggregate's transition function, which updates the running aggregate state with each new input value. The function sets up the necessary execution context, switches to the appropriate memory context, prepares the function call arguments with the current aggregate state and new input, invokes the transition function, and updates the aggregate state with the result. This function is optimized for pass-by-value types where data copying is minimal.

## Parameters / Member Variables
- `aggstate`: AggState pointer containing the overall aggregate execution state
- `pertrans`: AggStatePerTrans pointer containing per-transition function state and metadata
- `pergroup`: AggStatePerGroup pointer containing per-group aggregate values  
- `aggcontext`: ExprContext pointer providing the evaluation context for the aggregate
- `setno`: Integer indicating which grouping set is being processed

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCallInvoke
  - MemoryContextSwitchTo  
  - AggState (struct)
  - AggStatePerTrans (struct)
  - AggStatePerGroup (struct)
  - FunctionCallInfo (struct)
  - pg_attribute_always_inline
- Called from (representative examples):
  - ExecInterpExpr (multiple call sites)
  - EEO_JUMP (via jump table mechanism)

## Notes and Other Information  
- This function is marked as pg_attribute_always_inline for performance optimization
- Handles pass-by-value data types specifically (counterpart to ExecAggPlainTransByRef for pass-by-reference types)
- Sets up aggstate context fields (curaggcontext, current_set, curpertrans) for use by AggGetAggref()
- Executes transition function in per-tuple memory context to manage memory usage
- Updates both the aggregate value (transValue) and its null status (transValueIsNull)
- Part of PostgreSQL's expression evaluation step execution framework
- The function assumes the transition function is properly set up in pertrans->transfn_fcinfo