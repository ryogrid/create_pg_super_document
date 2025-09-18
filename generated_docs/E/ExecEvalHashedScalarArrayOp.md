# ExecEvalHashedScalarArrayOp

## Location
src/backend/executor/execExprInterp.c: 3670 - 3851

## Overview
Evaluates "scalar op ANY (const array)" expressions using a hash table for optimized repeat lookups, supporting only OR semantics and building a reusable hashtable on first evaluation.

## Definition
void ExecEvalHashedScalarArrayOp(ExprState *state, ExprEvalStep *op, ExprContext *econtext)

## Detailed Description
This function is an optimized version of ExecEvalScalarArrayOp that builds a hash table on the first lookup to accelerate subsequent evaluations of scalar array operations. The function handles "scalar op ANY (const array)" expressions where the array is constant and can be preprocessed into a hash table for O(1) lookups instead of O(n) linear searches.

The function operates in two phases:
1. **Hash table construction** (first call only): Extracts array elements, creates a hash table sized according to the number of elements, and populates it with non-null values while tracking null presence.
2. **Lookup phase** (every call): Performs hash table lookup of the scalar value and determines the result based on whether it's an IN or NOT IN clause.

Special handling is provided for:
- NULL scalar values with strict functions (returns NULL immediately)
- NULL array elements (tracked separately, not stored in hash table)
- NOT IN semantics with null handling for non-strict functions

## Parameters / Member Variables
- : Expression state context (unused in this function)
- : Expression evaluation step containing operation-specific data including hash table, function info, and array information
- : Expression evaluation context providing memory context for hash table allocation

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - ArrayGetNItems
  - get_typlenbyvalalign
  - saophash_create
  - saophash_insert
  - saophash_lookup
  - fmgr_info
  - InitFunctionCallInfoData
  - fetch_att
  - att_addlength_pointer
- Called from (representative examples):
  - ExecInterpExpr
  - FunctionReturningBool (via JIT compilation)

## Notes and Other Information
- Only supports OR semantics (ANY), unlike the general ExecEvalScalarArrayOp
- Hash table is allocated in ecxt_per_query_memory context for reuse across multiple evaluations
- Function assumes the array constant is not null (assertion check)
- Duplicate array values don't affect correctness but may result in slightly oversized hash tables
- The hash table stores only non-null array elements; null handling is done separately through the has_nulls flag