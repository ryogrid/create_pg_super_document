# transformCoalesceExpr

## Location
[src/backend/parser/parse_expr.c:2214-2262](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2214-L2262)

## Overview
Transforms a COALESCE expression from parse tree format into executable format, determining a common result type and applying necessary type coercions to all arguments.

## Definition
```c
static Node *
transformCoalesceExpr(ParseState *pstate, CoalesceExpr *c)
```

## Detailed Description
The `transformCoalesceExpr` function converts parsed COALESCE expressions into their executable representation during semantic analysis. COALESCE is a SQL function that returns the first non-NULL value among its arguments, making type consistency crucial for proper execution.

Key operations performed by this function:

1. **Argument Transformation**: Recursively transforms each argument expression using `transformExprRecurse()` to convert parse tree nodes into executable expressions
2. **Common Type Selection**: Uses `select_common_type()` to determine the most appropriate common type that all arguments can be coerced to, ensuring type consistency for the result
3. **Type Coercion**: Applies implicit type coercion to all arguments using `coerce_to_common_type()`, converting each argument to the determined common type
4. **Set-Returning Function Validation**: Enforces the restriction that set-returning functions (SRFs) cannot be used within COALESCE expressions, as this would violate SQL semantics
5. **Location Preservation**: Maintains source location information for accurate error reporting and debugging

The function ensures that COALESCE expressions follow SQL standard semantics while providing clear error messages for invalid usage patterns.

## Parameters / Member Variables
- `pstate`: Parse state containing context information for the current parsing operation, including SRF tracking
- `c`: The raw COALESCE expression from the parse tree to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new CoalesceExpr node)
  - transformExprRecurse (transforms individual argument expressions)
  - select_common_type (determines common type for all arguments)
  - coerce_to_common_type (applies implicit type coercion)
  - lappend (appends to linked lists)
  - lfirst (extracts values from list cells)
  - ereport/ERROR (error reporting)
  - parser_errposition (reports error location)
  - exprLocation (gets source location of expressions)

- Called from (representative examples):
  - transformExprRecurse (main expression transformation dispatcher)

## Notes and Other Information
- COALESCE requires at least one argument and all arguments must be coercible to a common type
- Set-returning functions (SRFs) are explicitly forbidden in COALESCE arguments to maintain scalar semantics
- The `coalescecollid` (collation ID) is set later during collation analysis, not in this function
- Type coercion uses implicit coercion rules, allowing automatic conversion between compatible types
- The function tracks SRF usage by comparing `p_last_srf` values before and after argument transformation
- Error messages suggest using LATERAL FROM clauses as an alternative when SRFs are needed
- The resulting expression will evaluate arguments left-to-right at runtime, returning the first non-NULL value
- All arguments are coerced to the common type regardless of whether they will be evaluated at runtime