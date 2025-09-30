# ExecJustScanVarVirt

## Location
[src/backend/executor/execExprInterp.c:2319-2325](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2319-L2325)

## Overview
An optimized expression evaluation function specifically designed for scanning variables from virtual tuple slots in PostgreSQL's expression interpreter.

## Definition

```c
static Datum
ExecJustScanVarVirt(ExprState *state, ExprContext *econtext, bool *isnull)
```
## Detailed Description
ExecJustScanVarVirt is a specialized version of ExecJustScanVar that is optimized for virtual tuple slots. This function is part of PostgreSQL's expression evaluation infrastructure and is used when the expression system can determine at compilation time that only virtual slots will be accessed for scanning variables.

The function serves as a thin wrapper around ExecJustVarVirtImpl, specifically accessing the scan tuple (ecxt_scantuple) from the expression context. This optimization avoids the overhead of checking slot types and tuple deforming that would be necessary in the general case, since virtual slots store values in a readily accessible array format.

This function is typically used in scenarios where PostgreSQL's query planner and expression compiler can guarantee that the scan tuple will always be a virtual slot, allowing for more efficient variable access during query execution.

## Parameters / Member Variables
- : ExprState containing the expression evaluation state and steps
- : Expression context containing tuple slots and other evaluation context
- : Output parameter that will be set to indicate if the retrieved value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ExecJustVarVirtImpl](ExecJustVarVirtImpl.md)
  - pg_attribute_always_inline (inline attribute)
- Called from (representative examples):
  - EEO_JUMP (expression evaluation jump table)
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (expression preparation)

## Notes and Other Information
- This function is marked as static and is only used within the expression interpreter
- It specifically accesses the scan tuple (ecxt_scantuple) from the expression context
- The function is part of a family of optimized variable access functions for different tuple slot types
- Uses ExecJustVarVirtImpl as the underlying implementation, which includes assertions to verify that the slot is indeed virtual and properly initialized
- This optimization is possible because virtual slots store values in arrays that can be directly accessed without tuple deforming

## Simplified Source

```c
static Datum
ExecJustScanVarVirt(ExprState *state, ExprContext *econtext, bool *isnull)
{
    // Optimized wrapper for virtual slots - direct array access
    return ExecJustVarVirtImpl(state, econtext->ecxt_scantuple, isnull);
}
```