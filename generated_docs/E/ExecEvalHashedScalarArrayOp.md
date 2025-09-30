# ExecEvalHashedScalarArrayOp

## Location
[src/backend/executor/execExprInterp.c:3670-3851](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExprInterp.c#L3670-L3851)

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
  - [ArrayGetNItems](../A/ArrayGetNItems.md)
  - [get_typlenbyvalalign](../g/get_typlenbyvalalign.md)
  - saophash_create
  - saophash_insert
  - saophash_lookup
  - [fmgr_info](../f/fmgr_info.md)
  - InitFunctionCallInfoData
  - [fetch_att](../f/fetch_att.md)
  - att_addlength_pointer
- Called from (representative examples):
  - [ExecInterpExpr](ExecInterpExpr.md)
  - [FunctionReturningBool](../F/FunctionReturningBool.md) (via JIT compilation)

## Notes and Other Information
- Only supports OR semantics (ANY), unlike the general ExecEvalScalarArrayOp
- [Hash](../H/Hash.md) table is allocated in ecxt_per_query_memory context for reuse across multiple evaluations
- Function assumes the array constant is not null (assertion check)
- Duplicate array values don't affect correctness but may result in slightly oversized hash tables
- The hash table stores only non-null array elements; null handling is done separately through the has_nulls flag

## Simplified Source

```c
void ExecEvalHashedScalarArrayOp(ExprState *state, ExprEvalStep *op, ExprContext *econtext)
{
    ScalarArrayOpExprHashTable *elements_tab = op->d.hashedscalararrayop.elements_tab;
    FunctionCallInfo fcinfo = op->d.hashedscalararrayop.fcinfo_data;
    bool inclause = op->d.hashedscalararrayop.inclause;
    bool strictfunc = op->d.hashedscalararrayop.finfo->fn_strict;
    Datum scalar = fcinfo->args[0].value;
    bool scalar_isnull = fcinfo->args[0].isnull;

    // Return NULL for strict functions with null scalar
    if (scalar_isnull && strictfunc)
    {
        *op->resnull = true;
        return;
    }

    // Build hash table on first evaluation
    if (elements_tab == NULL)
    {
        ArrayType *arr = DatumGetArrayTypeP(*op->resvalue);
        int nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
        bool has_nulls = false;

        // Allocate and initialize hash table
        elements_tab = create_and_setup_hash_table(op, econtext, nitems);

        // Populate hash table with array elements
        populate_hash_table_from_array(elements_tab, arr, &has_nulls);

        op->d.hashedscalararrayop.has_nulls = has_nulls;
    }

    // Perform hash lookup
    bool hashfound = (saophash_lookup(elements_tab->hashtab, scalar) != NULL);

    // Determine result based on IN/NOT IN clause
    bool result = inclause ? hashfound : !hashfound;
    bool resultnull = false;

    // Handle null array elements for non-matching cases
    if (!hashfound && op->d.hashedscalararrayop.has_nulls)
    {
        if (strictfunc)
        {
            // Strict function with nulls: return NULL
            result = false;
            resultnull = true;
        }
        else
        {
            // Execute function with null rhs
            result = execute_with_null_rhs(fcinfo, scalar, scalar_isnull, inclause);
            resultnull = fcinfo->isnull;
        }
    }

    *op->resvalue = BoolGetDatum(result);
    *op->resnull = resultnull;
}
```