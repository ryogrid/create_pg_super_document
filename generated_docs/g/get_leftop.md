# get_leftop

## Location
[src/include/nodes/nodeFuncs.h:83-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/nodeFuncs.h#L83-L94)

## Overview
A static inline utility function that extracts the left operand from a binary operator expression, or the only operand from a unary operator expression in PostgreSQL's parse tree.

## Definition
```c
static inline Node *
get_leftop(const void *clause)
```

## Detailed Description
This function provides access to the left (or first) argument of an OpExpr node in PostgreSQL's parse tree. For binary operators like '=', '<', '>', '+', etc., it returns the left operand. For unary operators like '-' (negation) or 'NOT', it returns the single operand. The function safely handles the case where the arguments list is empty by returning NULL. It uses the linitial() macro to access the first element of the arguments list, which is stored as a List structure in the OpExpr node.

## Parameters / Member Variables
- `clause`: A pointer to an OpExpr node (cast as void*). The caller is responsible for ensuring this is actually an OpExpr node.

## Dependencies
- Functions called/Symbols referenced:
  - [OpExpr](../O/OpExpr.md) (node type)
  - linitial (macro to get first list element)
  - NIL (empty list constant)
- Called from (representative examples):
  - [ExecIndexBuildScanKeys](../E/ExecIndexBuildScanKeys.md)
  - [MakeTidOpExpr](../M/MakeTidOpExpr.md)
  - [TidExprListCreate](../T/TidExprListCreate.md)
  - [addRangeClause](../a/addRangeClause.md)
  - [process_equivalence](../p/process_equivalence.md)
  - [match_clause_to_ordering_op](../m/match_clause_to_ordering_op.md)
  - [make_restrictinfo_internal](../m/make_restrictinfo_internal.md)
  - [mergejoinscansel](../m/mergejoinscansel.md)

## Notes and Other Information
- This is a static inline function defined in nodeFuncs.h, making it available to any file that includes this header
- The function assumes the input is a valid OpExpr node - no type checking is performed
- Returns NULL if the arguments list is empty (NIL)
- Commonly used in conjunction with get_rightop() to analyze binary operator expressions
- Critical for query optimization tasks like join planning, index selection, and selectivity estimation
- The returned Node* may need to be cast to the appropriate expression type depending on the context

## Simplified Source

```c
static inline Node *
get_leftop(const void *clause)
{
    const OpExpr *expr = (const OpExpr *) clause;

    // Return first argument if args list is not empty, otherwise NULL
    if (expr->args != NIL)
        return (Node *) linitial(expr->args);
    else
        return NULL;
}
```