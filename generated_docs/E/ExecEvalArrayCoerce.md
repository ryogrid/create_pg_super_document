# ExecEvalArrayCoerce

## Location
[src/backend/executor/execExprInterp.c:3059-3099](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L3059-L3099)

## Overview
Evaluates an ArrayCoerceExpr expression by converting an array from one type to another, either through binary-compatible type changes or by applying element-wise coercion.

## Definition

```c
void
ExecEvalArrayCoerce(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
```
## Detailed Description
ExecEvalArrayCoerce is an expression evaluation function that handles array type coercion in PostgreSQL's expression evaluation system. It operates on the result of a previous evaluation step (stored in the step's result variable) and performs one of two types of coercion:

1. **Binary-compatible coercion**: When the source and target element types are binary-compatible, it simply modifies the element type metadata in the array header without transforming the actual data.

2. **Element-wise coercion**: When elements need actual transformation, it uses the array_map function to apply a sub-expression to each array element, creating a new array with the coerced elements.

The function handles NULL arrays by returning NULL immediately, preserving SQL NULL semantics.

## Parameters / Member Variables
- : ExprState containing the overall expression evaluation context
- : ExprEvalStep containing the specific operation data including:
  - : Pointer to NULL flag for the result
  - : Pointer to the result value (input array datum)
  - : Sub-expression for element coercion (NULL for binary-compatible)
  - : Target element type OID
  - : Array map state for element transformation
- : ExprContext providing runtime evaluation context

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypePCopy: Creates a detoasted copy of the input array
  - ARR_ELEMTYPE: Macro to access/modify array element type
  - [array_map](../a/array_map.md): Applies element-wise transformation to array contents
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md): Main expression interpreter dispatch function
  - [FunctionReturningBool](../F/FunctionReturningBool.md): JIT compilation context

## Notes and Other Information
- This function is part of PostgreSQL's compiled expression evaluation system, designed for high-performance expression execution
- The binary-compatible optimization avoids expensive element-by-element processing when only type metadata needs to change
- Input arrays are always detoasted and copied to ensure proper memory management
- The function modifies the result in-place through the op->resvalue pointer
- Element-wise coercion leverages the array_map infrastructure for consistent array processing