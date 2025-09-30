# transformGroupingFunc

## Location
[src/backend/parser/parse_agg.c:260-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_agg.c#L260-L298)

## Overview
Transforms a GROUPING() expression during SQL parsing, treating it similarly to aggregate functions with respect to nesting level processing and validation.

## Definition
Node *transformGroupingFunc(ParseState *pstate, GroupingFunc *p)

## Detailed Description
This function processes GROUPING() expressions which are used in GROUP BY queries with ROLLUP, CUBE, or GROUPING SETS. The GROUPING function returns a bitmask indicating which grouping columns are included in the current grouping set. The function validates that the number of arguments doesn't exceed 31 (due to bitmask limitations), transforms each argument expression using the current parse state context, and applies the same level and nesting constraints as aggregate functions. It marks the parse state as having aggregates since GROUPING behaves like an aggregate function.

## Parameters / Member Variables
- `pstate`: Current parse state containing context information for the query being parsed
- `p`: The GroupingFunc node representing the GROUPING() expression to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [transformExpr](transformExpr.md)
  - [check_agglevels_and_constraints](../c/check_agglevels_and_constraints.md)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- GROUPING() is limited to fewer than 32 arguments due to its bitmask return value representation
- The function defers acceptability checking of expressions to later phases
- GROUPING() expressions are treated as aggregates for the purpose of query nesting validation
- The location information is preserved for error reporting purposes

## Simplified Source

```c
Node *
transformGroupingFunc(ParseState *pstate, GroupingFunc *p)
{
    List *args = p->args;
    List *result_list = NIL;
    GroupingFunc *result = makeNode(GroupingFunc);

    // Check argument count limit (bitmask representation requires < 32 args)
    if (list_length(args) > 31)
        ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                 errmsg("GROUPING must have fewer than 32 arguments"),
                 parser_errposition(pstate, p->location)));

    // Transform each argument expression
    foreach(lc, args)
    {
        Node *current_result = transformExpr(pstate, (Node *) lfirst(lc), pstate->p_expr_kind);
        result_list = lappend(result_list, current_result);
    }

    // Build the transformed GROUPING function
    result->args = result_list;
    result->location = p->location;

    // Apply aggregate-like nesting and level constraints
    check_agglevels_and_constraints(pstate, (Node *) result);

    return (Node *) result;
}
```