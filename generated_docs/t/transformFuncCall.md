# transformFuncCall

## Location
[src/backend/parser/parse_expr.c:1439-1483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L1439-L1483)

## Overview
Transforms a function call node (FuncCall) during parsing by processing its arguments and delegating to ParseFuncOrColumn for function resolution and type checking.

## Definition
```c
static Node *transformFuncCall(ParseState *pstate, FuncCall *fn)
```

## Detailed Description
The transformFuncCall function handles the transformation of function call expressions during SQL parsing. It processes both regular function arguments and special WITHIN GROUP syntax used with aggregate functions. The function transforms all arguments recursively, handles ORDER BY expressions for aggregate functions with WITHIN GROUP clauses, and then delegates the actual function resolution to ParseFuncOrColumn.

Key processing steps:
1. Transforms regular function arguments using transformExprRecurse
2. Handles WITHIN GROUP syntax by transforming ORDER BY expressions as additional arguments
3. Preserves the last set-returning function context for proper nesting validation
4. Delegates to ParseFuncOrColumn for function lookup, overload resolution, and type coercion

## Parameters / Member Variables
- `pstate`: ParseState context containing parsing state information and error handling context
- `fn`: FuncCall node containing function name, arguments, aggregate options, and location information

## Dependencies
- Functions called/Symbols referenced:
  - [FuncCall](../F/FuncCall.md) (struct type for function call expressions)
  - [SortBy](../S/SortBy.md) (struct type for ORDER BY expressions)
  - [transformExprRecurse](transformExprRecurse.md) (recursively transforms expression nodes)
  - [transformExpr](transformExpr.md) (transforms expressions with specific kind context)
  - [ParseFuncOrColumn](../P/ParseFuncOrColumn.md) (resolves function names and performs type checking)
  - EXPR_KIND_ORDER_BY (expression context constant)
- Called from:
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- This function is part of the SQL parser's expression transformation pipeline
- Handles special aggregate function syntax with WITHIN GROUP clauses
- Preserves set-returning function context to prevent invalid nesting
- The function is static, indicating it's only used within the parse_expr.c module
- ORDER BY expressions in WITHIN GROUP are treated as additional function arguments for type resolution
- Function overload resolution and type coercion are handled by ParseFuncOrColumn

## Simplified Source

```c
static Node *
transformFuncCall(ParseState *pstate, FuncCall *fn)
{
    Node *last_srf = pstate->p_last_srf;
    List *targs = NIL;

    // Transform regular function arguments
    foreach(args, fn->args)
    {
        targs = lappend(targs, transformExprRecurse(pstate, (Node *) lfirst(args)));
    }

    // Handle WITHIN GROUP syntax for aggregate functions
    if (fn->agg_within_group)
    {
        Assert(fn->agg_order != NIL);

        // Transform ORDER BY expressions as additional arguments
        foreach(args, fn->agg_order)
        {
            SortBy *arg = (SortBy *) lfirst(args);
            targs = lappend(targs, transformExpr(pstate, arg->node, EXPR_KIND_ORDER_BY));
        }
    }

    // Delegate to ParseFuncOrColumn for function resolution
    return ParseFuncOrColumn(pstate,
                             fn->funcname,
                             targs,
                             last_srf,
                             fn,
                             false,
                             fn->location);
}
```