# simplify_function

## Location
[src/backend/optimizer/util/clauses.c:4059-4174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L4059-L4174)

## Overview
Attempts to simplify a function call during constant expression evaluation by executing the function to deliver a constant result, using transform functions, or inlining SQL function bodies.

## Definition

```c
static Expr *
simplify_function(Oid funcid, Oid result_type, int32 result_typmod,
				  Oid result_collid, Oid input_collid, List **args_p,
				  bool funcvariadic, bool process_args, bool allow_non_const,
				  eval_const_expressions_context *context)
```
## Detailed Description
This function serves as a subroutine for eval_const_expressions and implements three strategies for function call simplification:

1. **Function execution**: Executes the function to deliver a constant result
2. **Transform functions**: Uses support functions to generate substitute node trees  
3. **Inline expansion**: Expands the body of simple SQL-language function definitions

The function also handles argument list processing, including converting named-notation arguments to positional notation and adding default argument expressions. This argument processing occurs even when function simplification itself is not possible.

The function accesses the pg_proc system catalog to retrieve function metadata needed for all simplification strategies.

## Parameters / Member Variables
- : OID of the function to simplify
- : Actual result type OID (needed for polymorphic functions)
- : Type modifier for the result
- : Collation OID for the result
- : Input collation to use for the function
- : Pointer to argument list (pass-by-reference for modification)
- : Whether the function is variadic
- : Whether to process arguments (convert named notation, add defaults)
- : Whether non-constant results are allowed (suppresses transform and inline strategies when false)
- : Context data for eval_const_expressions

## Dependencies
- Functions called/Symbols referenced:
  - [expand_function_arguments](../e/expand_function_arguments.md)
  - expression_tree_mutator
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md)
  - [evaluate_function](../e/evaluate_function.md)
  - [inline_function](../i/inline_function.md)
  - Form_pg_proc
  - [SupportRequestSimplify](../S/SupportRequestSimplify.md)
  - [FuncExpr](../F/FuncExpr.md)
- Called from (representative examples):
  - [eval_const_expressions_mutator](../e/eval_const_expressions_mutator.md)

## Notes and Other Information
- The function modifies the argument list in-place via the args_p parameter
- When allow_non_const is false, the function can only return a Const node or NULL
- Support functions are called via the prosupport field using a SupportRequestSimplify structure
- The function handles API safety by asserting that support functions don't return the dummy FuncExpr node
- Argument processing occurs regardless of whether function simplification succeeds

## Simplified Source

```c
static Expr *
simplify_function(Oid funcid, Oid result_type, int32 result_typmod,
                  Oid result_collid, Oid input_collid, List **args_p,
                  bool funcvariadic, bool process_args, bool allow_non_const,
                  eval_const_expressions_context *context)
{
    List *args = *args_p;
    HeapTuple func_tuple;
    Form_pg_proc func_form;
    Expr *newexpr;

    // Get function metadata from system catalog
    func_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(func_tuple))
        elog(ERROR, "cache lookup failed for function %u", funcid);
    func_form = (Form_pg_proc) GETSTRUCT(func_tuple);

    // Process function arguments if requested
    if (process_args) {
        // Convert named notation to positional, add defaults
        args = expand_function_arguments(args, false, result_type, func_tuple);
        // Recursively simplify argument expressions
        args = (List *) expression_tree_mutator((Node *) args,
                                               eval_const_expressions_mutator,
                                               (void *) context);
        *args_p = args;
    }

    // Strategy 1: Try to execute function for constant result
    newexpr = evaluate_function(funcid, result_type, result_typmod,
                               result_collid, input_collid,
                               args, funcvariadic,
                               func_tuple, context);

    // Strategy 2: Try support function transformation
    if (!newexpr && allow_non_const && OidIsValid(func_form->prosupport)) {
        SupportRequestSimplify req;
        FuncExpr fexpr;

        // Build dummy FuncExpr for support function interface
        fexpr.xpr.type = T_FuncExpr;
        fexpr.funcid = funcid;
        fexpr.funcresulttype = result_type;
        fexpr.funcretset = func_form->proretset;
        fexpr.funcvariadic = funcvariadic;
        fexpr.funcformat = COERCE_EXPLICIT_CALL;
        fexpr.funccollid = result_collid;
        fexpr.inputcollid = input_collid;
        fexpr.args = args;
        fexpr.location = -1;

        req.type = T_SupportRequestSimplify;
        req.root = context->root;
        req.fcall = &fexpr;

        newexpr = (Expr *) DatumGetPointer(OidFunctionCall1(func_form->prosupport,
                                                           PointerGetDatum(&req)));
    }

    // Strategy 3: Try function inlining
    if (!newexpr && allow_non_const)
        newexpr = inline_function(funcid, result_type, result_collid,
                                 input_collid, args, funcvariadic,
                                 func_tuple, context);

    ReleaseSysCache(func_tuple);
    return newexpr;
}
```