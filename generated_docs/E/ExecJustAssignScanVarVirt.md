# ExecJustAssignScanVarVirt

## Location
[src/backend/executor/execExprInterp.c:2362-2372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L2362-L2372)

## Overview
An optimized expression evaluation function for assigning variables from virtual scan tuple slots to result slots in PostgreSQL's query execution.

## Definition

```c
static Datum
ExecJustAssignScanVarVirt(ExprState *state, ExprContext *econtext, bool *isnull)
```
## Detailed Description
ExecJustAssignScanVarVirt is a specialized assignment function optimized for virtual tuple slots, specifically designed to handle scan tuple variables in query execution. This function is part of PostgreSQL's expression evaluation infrastructure and represents an optimization where the expression compiler can determine at compilation time that only virtual slots will be used for scan tuple access.

The function serves as a thin wrapper around ExecJustAssignVarVirtImpl, specifically targeting the scan tuple (ecxt_scantuple) from the expression context. This specialization allows PostgreSQL to generate more efficient code paths for operations where scan tuple access patterns are predictable and can benefit from virtual slot optimizations.

In query execution, the scan tuple typically represents the current tuple being processed from a table scan or index scan operation. By using this optimized function, PostgreSQL can avoid the overhead of general-purpose tuple slot access mechanisms when it knows the scan tuple will always be stored in a virtual slot format.

## Parameters / Member Variables
- `*state`: ExprState containing the expression evaluation state and operation steps
- `*econtext`: Expression context containing tuple slots for inner, outer, and scan tuples
- `*isnull`: Output parameter that will be set to indicate if the assigned value is NULL
## Dependencies
- Functions called/Symbols referenced:
  - [ExecJustAssignVarVirtImpl](ExecJustAssignVarVirtImpl.md)
  - EEO_USE_COMPUTED_GOTO (compilation directive)
- Called from (representative examples):
  - EEO_JUMP (expression evaluation jump table)
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (expression preparation)

## Notes and Other Information
- This function is marked as static and is only used within the expression interpreter
- Specifically accesses the scan tuple (ecxt_scantuple) from the expression context
- Part of a family of optimized variable assignment functions for different tuple contexts
- The optimization is possible because virtual slots store values in arrays that can be directly accessed and copied
- Used in scan operations where the tuples being scanned are guaranteed to be in virtual slot format
- Always returns the result from ExecJustAssignVarVirtImpl (typically 0 for assignment operations)
- Associated with computed goto optimizations in the expression interpreter

## Simplified Source

```c
static Datum
ExecJustAssignScanVarVirt(ExprState *state, ExprContext *econtext, bool *isnull)
{
    // Assign scan tuple variable to result slot using virtual slot optimization
    return ExecJustAssignVarVirtImpl(state, econtext->ecxt_scantuple, isnull);
}
```