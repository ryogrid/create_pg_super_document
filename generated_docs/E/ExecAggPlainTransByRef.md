# ExecAggPlainTransByRef

## Location
src/backend/executor/execExprInterp.c: 5269 - 5317

## Overview
This function implements the transition function invocation for aggregate operations on pass-by-reference data types, handling memory management and data copying for variable-length and complex data types.

## Definition
```c
static pg_attribute_always_inline void ExecAggPlainTransByRef(AggState *aggstate, AggStatePerTrans pertrans, AggStatePerGroup pergroup, ExprContext *aggcontext, int setno)
```

## Detailed Description
ExecAggPlainTransByRef is the pass-by-reference counterpart to ExecAggPlainTransByVal in PostgreSQL's aggregate execution framework. This function handles transition function calls for pass-by-reference data types such as text, bytea, arrays, and other variable-length or complex types. The key difference from the by-value version is the sophisticated memory management required for reference types: after invoking the transition function, it checks if the returned value is the same pointer as the input (indicating in-place modification) or a new value that needs to be copied into the appropriate memory context. It uses ExecAggCopyTransValue to handle the copying and freeing of the previous aggregate state value when necessary, ensuring proper memory context management throughout the aggregation process.

## Parameters / Member Variables
- `aggstate`: AggState pointer containing the overall aggregate execution state
- `pertrans`: AggStatePerTrans pointer containing per-transition function state and metadata  
- `pergroup`: AggStatePerGroup pointer containing per-group aggregate values
- `aggcontext`: ExprContext pointer providing the evaluation context for the aggregate
- `setno`: Integer indicating which grouping set is being processed

## Dependencies
- Functions called/Symbols referenced:
  - FunctionCallInvoke
  - [ExecAggCopyTransValue](ExecAggCopyTransValue.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [AggState](../A/AggState.md) (struct)
  - [AggStatePerTrans](../A/AggStatePerTrans.md) (struct)
  - [AggStatePerGroup](../A/AggStatePerGroup.md) (struct)  
  - [FunctionCallInfo](../F/FunctionCallInfo.md) (struct)
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md) (multiple call sites)
  - EEO_JUMP (via jump table mechanism)

## Notes and Other Information
- This function is marked as pg_attribute_always_inline for performance optimization
- Specifically handles pass-by-reference data types (counterpart to ExecAggPlainTransByVal for pass-by-value types)
- Includes sophisticated pointer comparison logic to detect when transition functions return modified input vs. new values
- Uses ExecAggCopyTransValue to manage memory context transitions and cleanup of old aggregate values
- The pointer comparison optimization avoids unnecessary copying when transition functions modify their input in-place
- Sets transValue to 0 when NULL to enable safe pointer comparisons without additional null checks
- Executes transition function in per-tuple memory context for proper memory management
- Part of PostgreSQL's expression evaluation step execution framework
- Critical for aggregates involving variable-length types like strings, arrays, and custom data types