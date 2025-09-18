# QTW_IGNORE_CTE_SUBQUERIES

## Location
[src/include/nodes/nodeFuncs.h:23-23](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/nodeFuncs.h#L23-L23)

## Overview
A flag bit used by query_tree_walker and query_tree_mutator functions to control whether subqueries in the CTE (Common Table Expression) list should be ignored during tree traversal.

## Definition
```c
#define QTW_IGNORE_CTE_SUBQUERIES    0x02    /* subqueries in cteList */
```

## Detailed Description
QTW_IGNORE_CTE_SUBQUERIES is a flag constant that controls the behavior of PostgreSQL's query tree traversal functions. When this flag is set, the tree walker and mutator functions will skip processing subqueries that are contained within the Common Table Expression (CTE) list of a query. This allows for selective traversal of query trees, enabling callers to avoid descending into CTE subqueries when they are not relevant to the current operation.

CTEs (WITH clauses) contain their own subqueries, and this flag provides a mechanism to exclude them from tree traversal operations when needed. This is particularly useful when performing operations that should only affect the main query structure and not the auxiliary CTE definitions.

## Parameters / Member Variables
- Value: `0x02` - Binary flag that can be combined with other QTW flags using bitwise OR operations

## Dependencies
- Used by:
  - [query_tree_walker_impl](../q/query_tree_walker_impl.md) (src/backend/nodes/nodeFuncs.c:2770)
  - [query_tree_mutator_impl](../q/query_tree_mutator_impl.md) (src/backend/nodes/nodeFuncs.c:3826)
  - [assign_query_collations](../a/assign_query_collations.md) (src/backend/parser/parse_collate.c:113)
- Part of the QTW flag system defined in src/include/nodes/nodeFuncs.h

## Notes and Other Information
- This flag specifically affects the cteList (Common Table Expression list) within Query nodes
- When set, CTE subqueries are skipped during tree traversal
- Can be combined with other QTW flags to create complex traversal patterns
- The flag is checked using bitwise AND operation: `!(flags & QTW_IGNORE_CTE_SUBQUERIES)`
- CTEs are processed before the main query structure in the traversal order
- Used in collation assignment to control which parts of the query tree are processed for collation inference