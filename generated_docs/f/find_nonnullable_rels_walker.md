# find_nonnullable_rels_walker

## Location
[src/backend/optimizer/util/clauses.c:1462-1706](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/clauses.c#L1462-L1706)

## Overview
The `find_nonnullable_rels_walker` function recursively traverses expression trees to identify which base relations are forced to be nonnullable by the given expression, supporting PostgreSQL's outer join optimization logic.

## Definition
```c
static Relids find_nonnullable_rels_walker(Node *node, bool top_level)
```

## Detailed Description
This function implements the core tree-walking logic for determining which relations cannot be all-NULL when an expression evaluates successfully. It performs detailed analysis of different node types to understand their strictness properties and how NULL values propagate through the expression tree.

The function handles two distinct contexts based on the `top_level` parameter:
- **Top level (true)**: Analyzing clauses where FALSE-or-NULL results are equivalent for determining nonnullable relations
- **Below top level (false)**: Analyzing within strict functions where NULL inputs must produce NULL outputs

Key analysis patterns include:

- **Variables**: Relations referenced by variables at the current query level are added to the result
- **Lists**: Union semantics - any arm that forces relations nonnullable contributes to the result
- **Strict functions/operators**: If function is strict, all argument relations become nonnullable
- **Boolean expressions**: Complex logic for AND/OR handling depends on top_level context
- **Type coercion nodes**: Transparent - pass through to the underlying expression
- **NULL tests**: IS NOT NULL tests make relations nonnullable at top level
- **SubPlans**: Special handling for ANY_SUBLINK and ROWCOMPARE_SUBLINK based on context
- **PlaceHolderVars**: Inherit nonnullability from contained expression, with special singleton handling

## Parameters / Member Variables
- `node`: A Node pointer representing the current expression node to analyze
- `top_level`: Boolean indicating whether this is top-level analysis (TRUE) or within a strict function context (FALSE)

## Dependencies
- Functions called/Symbols referenced:
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - [bms_join](../b/bms_join.md)
  - [bms_int_members](../b/bms_int_members.md)
  - bms_is_empty
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_membership](../b/bms_membership.md)
  - [func_strict](func_strict.md)
  - [set_opfuncid](../s/set_opfuncid.md)
  - [is_strict_saop](../i/is_strict_saop.md)
- Called from (representative examples):
  - [find_nonnullable_rels](find_nonnullable_rels.md)
  - max_parallel_hazard_context (self-recursively)

## Notes and Other Information
- Returns a Relids bitmapset containing relation OIDs that must be nonnullable
- Static function used internally within clauses.c
- Implements sophisticated logic for Boolean expression handling (AND vs OR semantics)
- Special optimization for early termination when intersection becomes empty in OR expressions
- Handles complex expression types including subqueries, type coercions, and placeholder variables
- Critical component of PostgreSQL's outer join elimination and optimization infrastructure
- Uses conservative analysis - safe to miss some nonnullable relations but must never incorrectly identify them
- Located in src/backend/optimizer/util/clauses.c:1462-1706

## Simplified Source

```c
static Relids
find_nonnullable_rels_walker(Node *node, bool top_level)
{
    Relids result = NULL;
    ListCell *l;

    if (node == NULL)
        return NULL;

    // Variable nodes: add relation to result if current level
    if (IsA(node, Var))
    {
        Var *var = (Var *) node;
        if (var->varlevelsup == 0)
            result = bms_make_singleton(var->varno);
    }
    // Lists: union semantics - combine results from all arms
    else if (IsA(node, List))
    {
        foreach(l, (List *) node)
        {
            result = bms_join(result,
                            find_nonnullable_rels_walker(lfirst(l), top_level));
        }
    }
    // Strict functions: if function is strict, arguments are nonnullable
    else if (IsA(node, FuncExpr))
    {
        FuncExpr *expr = (FuncExpr *) node;
        if (func_strict(expr->funcid))
            result = find_nonnullable_rels_walker((Node *) expr->args, false);
    }
    // Boolean expressions: complex AND/OR logic
    else if (IsA(node, BoolExpr))
    {
        BoolExpr *expr = (BoolExpr *) node;
        switch (expr->boolop)
        {
            case AND_EXPR:
                if (top_level)
                {
                    // At top level: union of arms
                    result = find_nonnullable_rels_walker((Node *) expr->args, top_level);
                    break;
                }
                // Fall through to OR logic below top level
            case OR_EXPR:
                // Intersection of all arms
                foreach(l, expr->args)
                {
                    Relids subresult = find_nonnullable_rels_walker(lfirst(l), top_level);
                    if (result == NULL)
                        result = subresult;
                    else
                        result = bms_int_members(result, subresult);

                    if (bms_is_empty(result))
                        break;
                }
                break;
            case NOT_EXPR:
                result = find_nonnullable_rels_walker((Node *) expr->args, false);
                break;
        }
    }
    // PlaceHolderVars: inherit from expression, add singleton phrels
    else if (IsA(node, PlaceHolderVar))
    {
        PlaceHolderVar *phv = (PlaceHolderVar *) node;
        result = find_nonnullable_rels_walker((Node *) phv->phexpr, top_level);

        if (phv->phlevelsup == 0 && bms_membership(phv->phrels) == BMS_SINGLETON)
            result = bms_add_members(result, phv->phrels);
    }
    // [Additional node types handling omitted for brevity]

    return result;
}
```