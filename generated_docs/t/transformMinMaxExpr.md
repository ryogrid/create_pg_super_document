# transformMinMaxExpr

## Location
[src/backend/parser/parse_expr.c:2263-2301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2263-L2301)

## Overview
Transforms GREATEST and LEAST expressions from parse tree format into executable format, determining a common result type and applying necessary type coercions to all arguments.

## Definition
```c
static Node *
transformMinMaxExpr(ParseState *pstate, MinMaxExpr *m)
```

## Detailed Description
The `transformMinMaxExpr` function converts parsed GREATEST and LEAST expressions into their executable representation during semantic analysis. These SQL functions return the maximum (GREATEST) or minimum (LEAST) value among their arguments, requiring type consistency across all arguments for proper comparison.

Key operations performed by this function:

1. **Operator Preservation**: Maintains the original operation type (`IS_GREATEST` or `IS_LEAST`) to determine the appropriate function name for error reporting
2. **Argument Transformation**: Recursively transforms each argument expression using `transformExprRecurse()` to convert parse tree nodes into executable expressions  
3. **Common Type Selection**: Uses `select_common_type()` to determine the most appropriate common type that all arguments can be coerced to, ensuring meaningful comparison operations
4. **Type Coercion**: Applies implicit type coercion to all arguments using `coerce_to_common_type()`, converting each argument to the determined common type
5. **Location Preservation**: Maintains source location information for accurate error reporting and debugging

The function ensures that GREATEST/LEAST expressions follow SQL standard semantics by establishing type compatibility among all arguments before execution.

## Parameters / Member Variables
- `pstate`: Parse state containing context information for the current parsing operation
- `m`: The raw MinMaxExpr from the parse tree to be transformed, containing the operator type and argument list

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new MinMaxExpr node)
  - [transformExprRecurse](transformExprRecurse.md) (transforms individual argument expressions)
  - [select_common_type](../s/select_common_type.md) (determines common type for all arguments)
  - [coerce_to_common_type](../c/coerce_to_common_type.md) (applies implicit type coercion)
  - [lappend](../l/lappend.md) (appends to linked lists)
  - lfirst (extracts values from list cells)
  - IS_GREATEST (constant for GREATEST operation type)

- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- The function supports both GREATEST and LEAST operations through the same transformation logic
- Function name selection is based on the `op` field: \"GREATEST\" for `IS_GREATEST`, \"LEAST\" otherwise
- All arguments must be coercible to a common type for the operation to be valid
- The `minmaxcollid` and `inputcollid` fields are set later during collation analysis, not in this function
- Type coercion uses implicit coercion rules, allowing automatic conversion between compatible types
- Unlike COALESCE, GREATEST/LEAST do not have restrictions on set-returning functions
- The resulting expression will perform comparison operations at runtime using the common type
- All arguments are coerced to the common type regardless of the actual comparison outcome
- The common type must support comparison operations (have appropriate btree operator classes)

## Simplified Source

```c
static Node *transformMinMaxExpr(ParseState *pstate, MinMaxExpr *m) {
    MinMaxExpr *newm = makeNode(MinMaxExpr);
    List *newargs = NIL;
    List *newcoercedargs = NIL;
    const char *funcname = (m->op == IS_GREATEST) ? "GREATEST" : "LEAST";

    // Copy operation type and transform all arguments
    newm->op = m->op;
    ListCell *args;
    foreach(args, m->args) {
        Node *e = (Node *) lfirst(args);
        Node *newe = transformExprRecurse(pstate, e);
        newargs = lappend(newargs, newe);
    }

    // Determine common type for all arguments
    newm->minmaxtype = select_common_type(pstate, newargs, funcname, NULL);

    // Convert all arguments to the common type
    foreach(args, newargs) {
        Node *e = (Node *) lfirst(args);
        Node *newe = coerce_to_common_type(pstate, e, newm->minmaxtype, funcname);
        newcoercedargs = lappend(newcoercedargs, newe);
    }

    // Set final properties and return
    newm->args = newcoercedargs;
    newm->location = m->location;
    // Note: minmaxcollid and inputcollid will be set by parse_collate.c

    return (Node *) newm;
}
```