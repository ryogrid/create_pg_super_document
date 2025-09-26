# JoinExpr

## Location
[src/include/nodes/primnodes.h:2277-2294](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/primnodes.h#L2277-L2294)

## Overview
JoinExpr represents SQL JOIN expressions in PostgreSQL's internal query tree, handling various types of joins including NATURAL, USING, and ON clauses with their respective semantics.

## Definition
```c
typedef struct JoinExpr
{
    NodeTag     type;
    JoinType    jointype;        /* type of join */
    bool        isNatural;       /* Natural join? Will need to shape table */
    Node       *larg;            /* left subtree */
    Node       *rarg;            /* right subtree */
    /* USING clause, if any (list of String) */
    List       *usingClause pg_node_attr(query_jumble_ignore);
    /* alias attached to USING clause, if any */
    Alias      *join_using_alias pg_node_attr(query_jumble_ignore);
    /* qualifiers on join, if any */
    Node       *quals;
    /* user-written alias clause, if any */
    Alias      *alias pg_node_attr(query_jumble_ignore);
    /* RT index assigned for join, or 0 */
    int         rtindex;
} JoinExpr;
```

## Detailed Description
JoinExpr is a crucial node type in PostgreSQL's join tree structure that represents SQL JOIN expressions. It sits above RangeTblRef leaf nodes in the join tree hierarchy and handles the complex semantics of different join types.

The structure manages the interdependent relationship between NATURAL, USING(), and ON() clauses. The SQL grammar enforces that only one of these can be written by the user. During parse analysis:
- NATURAL joins are converted to equivalent USING() lists, then filled with equality comparisons
- USING() clauses are filled with equality comparisons in the quals field
- ON() clauses directly populate only the quals field

JoinExpr supports join aliases through two mechanisms: the standard alias field that restricts visibility of tables/columns inside the join, and the join_using_alias introduced in SQL:2016 for JOIN/USING correlation names that include only common column names and don't restrict visibility.

During parse analysis, a Range Table Entry (RTE) is created for the join and its index is stored in rtindex. This allows Vars to reference the join's outputs. Planner-generated JoinExprs may have rtindex = 0 when no join alias variables reference them.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a JoinExpr node
- `jointype`: JoinType enum specifying the type of join (INNER, LEFT, RIGHT, FULL, etc.)
- `isNatural`: Boolean flag indicating if this is a NATURAL join
- `larg`: Pointer to the left subtree node in the join
- `rarg`: Pointer to the right subtree node in the join
- `usingClause`: List of String nodes representing column names in USING clause
- `join_using_alias`: Alias node for SQL:2016 JOIN/USING correlation names
- `quals`: Node containing join qualification expressions (WHERE conditions)
- `alias`: User-written alias clause that restricts visibility within the join
- `rtindex`: Range table index for this join, or 0 for planner-generated joins

## Dependencies
- Functions called/Symbols referenced:
  - JoinType (enumeration for join types)
  - [Alias](../A/Alias.md) (for alias handling)
  - NodeTag (for node identification)
  - [Node](../N/Node.md) (for subtree references)
  - [List](../L/List.md) (for USING clause column names)
- Called from (representative examples):
  - [add_base_rels_to_query](../a/add_base_rels_to_query.md) (initsplan.c:175)
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (parse_clause.c:1151)
  - [pull_up_subqueries_recurse](../p/pull_up_subqueries_recurse.md) (prepjointree.c:979)
  - [get_from_clause_item](../g/get_from_clause_item.md) (ruleutils.c:12199)
  - [deconstruct_recurse](../d/deconstruct_recurse.md) (initsplan.c:906)
  - [reduce_outer_joins_pass1](../r/reduce_outer_joins_pass1.md) (prepjointree.c:3040)

## Notes and Other Information
- [JoinExpr](JoinExpr.md) nodes form the internal nodes of join trees, with RangeTblRef as leaves
- The interdependence of isNatural, usingClause, and quals fields requires careful handling during parse analysis
- [Join](Join.md) aliases have significant semantic impact on column visibility and must be handled correctly
- The rtindex field enables proper variable referencing in complex join expressions
- Planner-generated JoinExprs may not have corresponding range table entries
- The structure supports both traditional SQL joins and newer SQL:2016 JOIN/USING correlation names
- NATURAL and USING joins affect the output column list differently than ON clauses