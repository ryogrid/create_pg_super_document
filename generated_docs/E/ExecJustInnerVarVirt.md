# ExecJustInnerVarVirt

## Location
src/backend/executor/execExprInterp.c: 2305 - 2311

## Overview
ExecJustInnerVarVirt is a specialized function for efficiently accessing variables from the inner relation tuple in join operations when using virtual tuple slots.

## Definition
```c
static Datum ExecJustInnerVarVirt(ExprState *state, ExprContext *econtext, bool *isnull)
```

## Detailed Description
This function is an optimized wrapper around ExecJustVarVirtImpl specifically designed for accessing variables from the inner relation in join operations. It is part of PostgreSQL's expression evaluation optimization system that provides specialized handlers for common access patterns.

The function delegates the actual work to ExecJustVarVirtImpl, passing the inner tuple slot from the expression context. This specialization allows PostgreSQL to avoid the overhead of determining which tuple slot to use at runtime, since it's known at compilation time that this expression accesses the inner relation.

## Parameters / Member Variables
- `state`: ExprState structure containing the expression evaluation steps and variable information
- `econtext`: ExprContext providing access to the inner tuple slot and other evaluation context
- `isnull`: Pointer to boolean flag that will be set to indicate if the variable value is NULL

## Dependencies
- Functions called/Symbols referenced:
  - [ExecJustVarVirtImpl](ExecJustVarVirtImpl.md) (core implementation for virtual slot variable access)
- Called from (representative examples):
  - EEO_JUMP (expression evaluation dispatch mechanism)
  - [ExecReadyInterpretedExpr](ExecReadyInterpretedExpr.md) (expression preparation function)

## Notes and Other Information
- This is a static function within execExprInterp.c, part of the internal expression evaluation machinery
- Optimized specifically for virtual slots, avoiding tuple deforming overhead
- Part of PostgreSQL's "just-in-time" expression evaluation system that provides specialized handlers for different tuple slot types
- Works in conjunction with join execution nodes that maintain inner and outer tuple contexts
- The "Virt" suffix indicates this is the virtual slot optimized version, contrasting with the general ExecJustInnerVar function