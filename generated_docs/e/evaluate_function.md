# evaluate_function

## Location
[src/backend/optimizer/util/clauses.c:4425-4550](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L4425-L4550)

## Overview
Attempts to pre-evaluate a function call during query optimization by checking if the function can be simplified to a constant value based on its inputs and volatility properties.

## Definition

```c
static Expr *
evaluate_function(Oid funcid, Oid result_type, int32 result_typmod,
				  Oid result_collid, Oid input_collid, List *args,
				  bool funcvariadic,
				  HeapTuple func_tuple,
				  eval_const_expressions_context *context)
```
## Detailed Description
This function performs constant folding optimization on function calls. It can simplify function calls in two main scenarios:
1. For strict functions with any constant-NULL inputs: returns a NULL constant since the function will never be called
2. For immutable functions (or stable functions in estimation mode) with all constant inputs: actually evaluates the function and returns the result as a Const node

The function includes several safety checks to prevent simplification when inappropriate, such as functions that return sets, functions that return RECORD type, or functions with non-constant inputs. It respects PostgreSQL's function volatility categories and only evaluates immutable functions normally, though it allows stable function evaluation during estimation phases.

## Parameters / Member Variables
- : OID of the function to evaluate
- : Expected result type OID of the function
- : Type modifier for the result
- : Collation ID for the result
- : Collation ID for the inputs
- : List of function arguments
- : Whether the function is variadic
- : HeapTuple containing the function's catalog entry
- : Evaluation context containing optimization settings

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_proc (function catalog entry structure)
  - [makeNullConst](../m/makeNullConst.md) (creates NULL constant nodes)
  - PROVOLATILE_IMMUTABLE, PROVOLATILE_STABLE (volatility constants)
  - [FuncExpr](../F/FuncExpr.md) (function expression node type)
  - COERCE_EXPLICIT_CALL (coercion type constant)
  - [evaluate_expr](evaluate_expr.md) (actually evaluates the expression)
- Called from:
  - [simplify_function](../s/simplify_function.md) (main function simplification routine)

## Notes and Other Information
- Returns NULL if the function cannot be simplified, otherwise returns a simplified Expr
- Cannot simplify functions that return sets (proretset = true)
- Cannot simplify functions that return RECORD type due to tuple descriptor complexity
- For strict functions, any NULL input results in immediate NULL output optimization
- In estimation mode, stable functions can be evaluated in addition to immutable ones
- The function builds a temporary FuncExpr node before calling evaluate_expr to perform the actual evaluation
- Located in src/backend/optimizer/util/clauses.c at lines 4425-4550

## Simplified Source

```c
static Expr *
evaluate_function(Oid funcid, Oid result_type, int32 result_typmod,
                  Oid result_collid, Oid input_collid, List *args,
                  bool funcvariadic, HeapTuple func_tuple,
                  eval_const_expressions_context *context)
{
    Form_pg_proc funcform = (Form_pg_proc) GETSTRUCT(func_tuple);
    bool has_nonconst_input = false;
    bool has_null_input = false;
    ListCell *arg;
    FuncExpr *newexpr;

    // Can't simplify functions that return sets or RECORD type
    if (funcform->proretset || funcform->prorettype == RECORDOID)
        return NULL;

    // Check all arguments for constants and NULLs
    foreach(arg, args)
    {
        if (IsA(lfirst(arg), Const))
            has_null_input |= ((Const *) lfirst(arg))->constisnull;
        else
            has_nonconst_input = true;
    }

    // Strict function with NULL input always returns NULL
    if (funcform->proisstrict && has_null_input)
        return (Expr *) makeNullConst(result_type, result_typmod, result_collid);

    // Need all constant inputs to proceed
    if (has_nonconst_input)
        return NULL;

    // Only evaluate immutable functions (or stable in estimation mode)
    if (funcform->provolatile == PROVOLATILE_IMMUTABLE ||
        (context->estimate && funcform->provolatile == PROVOLATILE_STABLE))
    {
        // Build FuncExpr node with simplified arguments
        newexpr = makeNode(FuncExpr);
        newexpr->funcid = funcid;
        newexpr->funcresulttype = result_type;
        newexpr->funcretset = false;
        newexpr->funcvariadic = funcvariadic;
        newexpr->funcformat = COERCE_EXPLICIT_CALL;
        newexpr->funccollid = result_collid;
        newexpr->inputcollid = input_collid;
        newexpr->args = args;
        newexpr->location = -1;

        // Actually evaluate the function call
        return evaluate_expr((Expr *) newexpr, result_type, result_typmod, result_collid);
    }

    return NULL;
}
```