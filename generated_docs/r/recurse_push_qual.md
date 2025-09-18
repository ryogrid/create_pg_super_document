# recurse_push_qual

## Location
src/backend/optimizer/path/allpaths.c: 4003 - 4054

## Overview
This helper function recursively traverses a set operations tree (UNION, INTERSECT, EXCEPT) to push a restriction clause down into each component subquery within the tree structure.

## Definition
```c
static void recurse_push_qual(Node *setOp, Query *topquery, RangeTblEntry *rte, Index rti, Node *qual)
```

## Detailed Description
This function implements the recursive traversal logic needed to push qualifiers down through PostgreSQL's set operation trees. Set operations in PostgreSQL are represented as binary trees where each node can be either a leaf (RangeTblRef pointing to a subquery) or an internal node (SetOperationStmt representing a set operation like UNION).

The function handles two cases:

**Leaf Nodes (RangeTblRef)**: When encountering a leaf node, it:
- Extracts the RangeTblRef and uses it to fetch the corresponding RangeTblEntry from the top query's range table
- Retrieves the actual subquery from the RangeTblEntry
- Calls subquery_push_qual() to push the restriction clause into this specific subquery

**Internal Nodes (SetOperationStmt)**: When encountering an internal set operation node, it:
- Recursively calls itself on both the left argument (larg) and right argument (rarg)
- This ensures the qualifier is pushed down to all leaf subqueries in the entire tree

The recursive approach ensures that every component subquery within complex nested set operations receives a copy of the restriction clause, maintaining query semantics while enabling optimization opportunities.

## Parameters / Member Variables
- `setOp`: Node representing either a RangeTblRef (leaf) or SetOperationStmt (internal node) in the set operations tree
- `topquery`: The top-level query containing the set operations tree and range table
- `rte`: The RangeTblEntry for the subquery in the parent query (passed through to subquery_push_qual)
- `rti`: The range table index of the subquery in the parent query (passed through to subquery_push_qual)
- `qual`: The restriction clause node to be pushed down to each component subquery

## Dependencies
- Functions called/Symbols referenced:
  - IsA (macro)
  - rt_fetch
  - subquery_push_qual
  - recurse_push_qual (recursive self-calls)
  - elog
  - nodeTag
- Types referenced:
  - RangeTblRef
  - SetOperationStmt
  - RangeTblEntry
  - Query
- Called from (representative examples):
  - subquery_push_qual (src/backend/optimizer/path/allpaths.c:3961)
  - recurse_push_qual (recursive calls at src/backend/optimizer/path/allpaths.c:4019-4020)

## Notes and Other Information
- Static helper function within allpaths.c, specifically designed for set operations tree traversal
- Implements depth-first traversal of the binary set operations tree
- Includes error handling for unexpected node types that shouldn't appear in set operations trees
- Essential for ensuring qualifier pushdown works correctly with complex nested set operations
- Each component subquery gets its own copy of the restriction clause through the recursive process
- Part of PostgreSQL's broader qualifier pushdown optimization framework
- Located in src/backend/optimizer/path/allpaths.c:4003-4054