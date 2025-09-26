# FromExpr

## Location
src/include/nodes/primnodes.h: 2305 - 2310

## Overview
FromExpr represents a FROM ... WHERE ... construct in PostgreSQL's join tree, providing a flexible container that can hold any number of join subtrees and qualification expressions.

## Definition
```c
typedef struct FromExpr
{
    NodeTag     type;
    List       *fromlist;        /* List of join subtrees */
    Node       *quals;           /* qualifiers on join, if any */
} FromExpr;
```

## Detailed Description
FromExpr is a fundamental node type in PostgreSQL's join tree structure that represents FROM ... WHERE ... constructs. It serves as a more flexible alternative to JoinExpr, capable of handling any number of children (including zero), while being less complex since it doesn't need to manage aliases and other join-specific semantics.

The key characteristic of FromExpr is its flexibility in representing cross-product joins ("FROM foo, bar, baz WHERE ..."). It functions similarly to a JoinExpr of jointype JOIN_INNER, but can accommodate multiple child nodes rather than being restricted to just two.

An important architectural note is that the top level of a Query's jointree is always a FromExpr, even if the jointree contains no relations. This provides a consistent structure for query processing. The output column set is implicitly the union of outputs from all child nodes.

The qualification expressions in FromExpr nodes work in conjunction with those in JoinExpr nodes. The position of qualifications is critical when outer joins are present, as enforcing a qualification too early or too late can cause outer joins to produce incorrect NULL-extended rows. However, when all joins are inner joins, the qualification positions become semantically interchangeable.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a FromExpr node
- `fromlist`: List containing join subtrees (can include RangeTblRef, JoinExpr, or other FromExpr nodes)
- `quals`: Node containing qualification expressions (WHERE clause conditions)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (for node identification)
  - List (for storing join subtrees)
  - Node (for qualification expressions)
- Called from (representative examples):
  - makeFromExpr (makefuncs.c:336)
  - add_base_rels_to_query (initsplan.c:167)
  - deconstruct_jointree (initsplan.c:760)
  - pull_up_sublinks (prepjointree.c:467)
  - pull_up_subqueries (prepjointree.c:937)
  - reduce_outer_joins_pass1 (prepjointree.c:3024)

## Notes and Other Information
- FromExpr always serves as the top-level node in a Query's jointree structure
- More flexible than JoinExpr in terms of number of children, but simpler in terms of alias handling
- The output column set is implicitly the union of all child node outputs
- Essential for representing cross-product joins with multiple tables
- Qualification positioning is critical for correct outer join semantics
- Can contain zero children, providing a consistent structure even for queries without relations
- Works in conjunction with JoinExpr nodes to form complete join tree hierarchies
- The fromlist can contain a mix of different node types (RangeTblRef, JoinExpr, other FromExpr nodes)