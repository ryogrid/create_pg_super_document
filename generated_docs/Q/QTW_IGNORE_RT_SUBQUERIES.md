# QTW_IGNORE_RT_SUBQUERIES

## Location
[src/include/nodes/nodeFuncs.h:22-22](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/nodeFuncs.h#L22-L22)

## Overview
A flag bit used by query_tree_walker and query_tree_mutator functions to control whether subqueries in the range table should be ignored during tree traversal.

## Definition


## Detailed Description
QTW_IGNORE_RT_SUBQUERIES is a flag constant that controls the behavior of PostgreSQL's query tree traversal functions. When this flag is set, the tree walker and mutator functions will skip processing subqueries that are contained within range table entries (RTE_SUBQUERY type entries). This provides a mechanism to perform selective traversal of the query tree, allowing callers to avoid descending into subqueries within the range table when they are not relevant to the current operation.

The flag is part of a broader set of QTW (Query Tree Walker) flags that provide fine-grained control over which parts of a query tree are processed during traversal operations.

## Parameters / Member Variables
- Value:  - Binary flag that can be combined with other QTW flags using bitwise OR operations

## Dependencies
- Used by: 
  - [range_table_entry_walker_impl](../r/range_table_entry_walker_impl.md) (src/backend/nodes/nodeFuncs.c:2831)
  - [range_table_mutator_impl](../r/range_table_mutator_impl.md) (src/backend/nodes/nodeFuncs.c:3863)
- Part of the QTW flag system defined in src/include/nodes/nodeFuncs.h

## Notes and Other Information
- This flag specifically affects RTE_SUBQUERY type range table entries
- When set, subqueries within range table entries are skipped during tree traversal
- Can be combined with other QTW flags to create complex traversal patterns
- The flag is checked using bitwise AND operation: 
- Part of PostgreSQL's node traversal infrastructure used throughout the query planner and optimizer