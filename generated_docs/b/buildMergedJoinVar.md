# buildMergedJoinVar

## Location
[src/backend/parser/parse_clause.c:1666-1773](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L1666-L1773)

## Overview
Generates a suitable replacement expression for a merged join column, handling type coercion and join-type-specific logic for USING clause columns.

## Definition
```c
static Node *buildMergedJoinVar(ParseState *pstate, JoinType jointype,
                               Var *l_colvar, Var *r_colvar)
```

## Detailed Description
This function creates a unified expression for columns that appear in a JOIN USING clause, where the same-named columns from both sides of the join need to be merged into a single output column. It first determines the common output type and typmod using select_common_type and select_common_typmod, ensuring type compatibility between the left and right column variables. The function then applies necessary type coercions: if types differ, it uses coerce_type for explicit conversion; if only typmod differs, it applies makeRelabelType for implicit relabeling. The core logic varies by join type: for INNER joins, it prefers non-coerced variables when available; for LEFT joins, it always uses the left variable; for RIGHT joins, it always uses the right variable; for FULL OUTER joins, it constructs a COALESCE expression to handle null values from either side. Finally, it calls assign_expr_collations to ensure proper collation information is applied to any coercion or CoalesceExpr nodes created during the process.

## Parameters / Member Variables
- `pstate`: ParseState containing current parsing context for type resolution and coercion
- `jointype`: JoinType indicating the type of join (INNER, LEFT, RIGHT, FULL)
- `l_colvar`: Var representing the column from the left side of the join
- `r_colvar`: Var representing the column from the right side of the join

## Dependencies
- Functions called/Symbols referenced:
  - [select_common_type](../s/select_common_type.md)
  - [select_common_typmod](../s/select_common_typmod.md)
  - [coerce_type](../c/coerce_type.md)
  - [makeRelabelType](../m/makeRelabelType.md)
  - [assign_expr_collations](../a/assign_expr_collations.md)
  - list_make2
- Types referenced:
  - JoinType
  - [Var](../V/Var.md)
  - [CoalesceExpr](../C/CoalesceExpr.md)
- Constants referenced:
  - JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_FULL
  - COERCION_IMPLICIT, COERCE_IMPLICIT_CAST
- Called from (representative examples):
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (for USING clause processing)

## Notes and Other Information
- This is a static function within parse_clause.c used internally for JOIN USING processing
- Critical for implementing SQL standard semantics for USING clause columns
- Handles type compatibility and coercion automatically between different column types
- For FULL OUTER joins, creates COALESCE expressions to properly handle NULL values
- The function ensures proper collation information is maintained through type coercions
- Essential for correct behavior of JOIN USING clauses with mixed column types
- Always applies assign_expr_collations to maintain proper collation semantics in the result

## Simplified Source

```c
static Node *
buildMergedJoinVar(ParseState *pstate, JoinType jointype,
                   Var *l_colvar, Var *r_colvar)
{
    // Determine common output type and typmod for both columns
    Oid outcoltype = select_common_type(pstate,
                                        list_make2(l_colvar, r_colvar),
                                        "JOIN/USING", NULL);
    int32 outcoltypmod = select_common_typmod(pstate,
                                              list_make2(l_colvar, r_colvar),
                                              outcoltype);

    // Apply type coercion to left column if needed
    Node *l_node;
    if (l_colvar->vartype != outcoltype)
        l_node = coerce_type(pstate, (Node *) l_colvar, l_colvar->vartype,
                             outcoltype, outcoltypmod,
                             COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    else if (l_colvar->vartypmod != outcoltypmod)
        l_node = (Node *) makeRelabelType((Expr *) l_colvar,
                                          outcoltype, outcoltypmod,
                                          InvalidOid, COERCE_IMPLICIT_CAST);
    else
        l_node = (Node *) l_colvar;

    // Apply type coercion to right column if needed
    Node *r_node;
    if (r_colvar->vartype != outcoltype)
        r_node = coerce_type(pstate, (Node *) r_colvar, r_colvar->vartype,
                             outcoltype, outcoltypmod,
                             COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
    else if (r_colvar->vartypmod != outcoltypmod)
        r_node = (Node *) makeRelabelType((Expr *) r_colvar,
                                          outcoltype, outcoltypmod,
                                          InvalidOid, COERCE_IMPLICIT_CAST);
    else
        r_node = (Node *) r_colvar;

    // Choose output based on join type
    Node *res_node;
    switch (jointype)
    {
        case JOIN_INNER:
            // Prefer non-coerced variable if available
            if (IsA(l_node, Var))
                res_node = l_node;
            else if (IsA(r_node, Var))
                res_node = r_node;
            else
                res_node = l_node;
            break;
        case JOIN_LEFT:
            res_node = l_node;
            break;
        case JOIN_RIGHT:
            res_node = r_node;
            break;
        case JOIN_FULL:
            // Create COALESCE to handle nulls from either side
            CoalesceExpr *c = makeNode(CoalesceExpr);
            c->coalescetype = outcoltype;
            c->args = list_make2(l_node, r_node);
            c->location = -1;
            res_node = (Node *) c;
            break;
        default:
            elog(ERROR, "unrecognized join type: %d", (int) jointype);
            res_node = NULL;
            break;
    }

    // Apply collation information to coercion/CoalesceExpr nodes
    assign_expr_collations(pstate, res_node);

    return res_node;
}
```