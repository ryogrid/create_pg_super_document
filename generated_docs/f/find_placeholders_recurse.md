# find_placeholders_recurse

## Location
[src/backend/optimizer/util/placeholder.c:207-256](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/placeholder.c#L207-L256)

## Overview
Recursively traverses the query jointree structure to systematically discover PlaceHolderVars in all join qualifications and expressions throughout the tree hierarchy.

## Definition
```c
static void find_placeholders_recurse(PlannerInfo *root, Node *jtnode)
```

## Detailed Description
The `find_placeholders_recurse` function implements the core recursive logic for walking through PostgreSQLs join tree data structure to locate PlaceHolderVars. It handles three main types of join tree nodes: RangeTblRef (base table references), FromExpr (implicit joins and WHERE clauses), and JoinExpr (explicit joins). For each node type, it first recursively processes child nodes, then examines any qualification expressions using `find_placeholders_in_expr`.

The function follows a depth-first traversal pattern, ensuring that all nested join structures are properly examined. It processes both explicit join conditions (in JoinExpr nodes) and implicit join conditions or WHERE clauses (in FromExpr nodes).

## Parameters / Member Variables
- `root`: PlannerInfo structure containing planning context and placeholder management
- `jtnode`: Current jointree node being examined (can be RangeTblRef, FromExpr, or JoinExpr)

## Dependencies
- Functions called/Symbols referenced:
  - [find_placeholders_recurse](find_placeholders_recurse.md) (recursive self-calls for child nodes)
  - [find_placeholders_in_expr](find_placeholders_in_expr.md) (for processing qualification expressions)
  - RangeTblRef, FromExpr, JoinExpr (join tree node types)
  - nodeTag (for error reporting on unrecognized node types)
- Called from (representative examples):
  - [find_placeholders_in_jointree](find_placeholders_in_jointree.md) (initial call)
  - [find_placeholders_recurse](find_placeholders_recurse.md) (recursive calls)

## Notes and Other Information
- Static function, only accessible within placeholder.c
- Handles NULL jtnode gracefully by returning immediately
- RangeTblRef nodes require no processing as they have no embedded qualifications
- FromExpr nodes process fromlist children first, then top-level quals (WHERE clauses)
- JoinExpr nodes process left and right arguments first, then join quals
- Throws ERROR for unrecognized node types to catch programming errors
- Implements depth-first traversal to ensure complete coverage of join tree
- Critical component of the placeholder discovery phase during query planning