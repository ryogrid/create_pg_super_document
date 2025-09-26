# ExecJustVarImpl

## Location
[src/backend/executor/execExprInterp.c:2150-2166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2150-L2166)

## Overview
ExecJustVarImpl is a fast-path implementation function for simple variable expressions, providing optimized attribute retrieval from tuple slots.

## Definition

```c
static pg_attribute_always_inline Datum
ExecJustVarImpl(ExprState *state, TupleTableSlot *slot, bool *isnull)
```
## Detailed Description
This function serves as the core implementation for simple variable access operations (ExecJustInnerVar, ExecJustOuterVar, ExecJustScanVar). It's designed as a fast-path optimization for expressions that consist of just a single variable reference without complex operations.

The function performs minimal overhead operations:
1. Retrieves the variable's attribute number from the expression state
2. Validates slot compatibility in debug builds
3. Directly calls slot_getattr() to fetch the attribute value

The implementation is marked as pg_attribute_always_inline to ensure it gets inlined at call sites, eliminating function call overhead for these common simple expressions.

## Parameters / Member Variables
- : Pointer to ExprState containing the expression evaluation context and steps
- : Pointer to TupleTableSlot from which to retrieve the attribute value
- : Pointer to boolean that will be set to indicate if the retrieved value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ExprEvalStep](ExprEvalStep.md) (structure access)
  - [CheckOpSlotCompatibility](../C/CheckOpSlotCompatibility.md)
  - [slot_getattr](../s/slot_getattr.md)
- Called from (representative examples):
  - [ExecJustInnerVar](ExecJustInnerVar.md)
  - [ExecJustOuterVar](ExecJustOuterVar.md)  
  - [ExecJustScanVar](ExecJustScanVar.md)

## Notes and Other Information
- Located in src/backend/executor/execExprInterp.c:2150-2166
- Always inlined (pg_attribute_always_inline) for maximum performance
- Part of PostgreSQL's fast-path expression evaluation system
- Relies on slot_getattr() for bounds checking and error handling
- Eliminates need for explicit FETCHSOME step implementation
- Used for the most common case of simple variable expressions in queries