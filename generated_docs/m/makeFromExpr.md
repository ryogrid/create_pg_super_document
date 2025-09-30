# makeFromExpr

## Location
[src/backend/nodes/makefuncs.c:334-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L334-L347)

## Overview
Creates a FromExpr node representing the FROM clause of a query along with associated WHERE clause qualifications.

## Definition
```c
FromExpr *makeFromExpr(List *fromlist, Node *quals)
```

## Detailed Description
The `makeFromExpr` function allocates and initializes a new FromExpr node, which represents a FROM clause in PostgreSQL's internal query tree structure. A FromExpr combines a list of relations (tables, subqueries, joins, etc.) that appear in the FROM clause with the qualification expressions (WHERE clause conditions) that apply to those relations.

This node type is fundamental in PostgreSQL's query processing, serving as a container that groups together the data sources and their filtering conditions. It's used extensively during query parsing, planning, and optimization phases.

## Parameters / Member Variables
- `fromlist`: List of relations and join expressions that make up the FROM clause (can be tables, subqueries, join nodes, etc.)
- `quals`: Qualification expressions (WHERE clause conditions) that apply to the relations in the fromlist (can be NULL if no WHERE clause)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (macro for node allocation)  
  - [FromExpr](../F/FromExpr.md) (node type being created)
  - [List](../L/List.md) (PostgreSQL's generic list type)
  - [Node](../N/Node.md) (base node type)
- Called from (representative examples):
  - [transformSelectStmt](../t/transformSelectStmt.md) (parser)
  - [transformDeleteStmt](../t/transformDeleteStmt.md) (parser) 
  - [transformUpdateStmt](../t/transformUpdateStmt.md) (parser)
  - [transformMergeStmt](../t/transformMergeStmt.md) (parser)
  - [pull_up_sublinks](../p/pull_up_sublinks.md) (optimizer)
  - [remove_useless_results_recurse](../r/remove_useless_results_recurse.md) (optimizer)

## Notes and Other Information
- [FromExpr](../F/FromExpr.md) nodes are central to representing SQL FROM clauses with associated WHERE conditions
- The `fromlist` can contain various node types including RangeTblRef, JoinExpr, and other FromExpr nodes
- The `quals` field typically contains AND/OR expressions that filter the Cartesian product of the relations in `fromlist`
- Used extensively throughout the parser for different statement types (SELECT, UPDATE, DELETE, MERGE)
- Also used in query optimization phases for subquery pullup and join processing
- Located in src/backend/nodes/makefuncs.c:334-347

## Simplified Source

```c
FromExpr *makeFromExpr(List *fromlist, Node *quals) {
    // Create new FromExpr node
    FromExpr *f = makeNode(FromExpr);

    // Set FROM clause relations and WHERE clause conditions
    f->fromlist = fromlist;
    f->quals = quals;

    return f;
}
```