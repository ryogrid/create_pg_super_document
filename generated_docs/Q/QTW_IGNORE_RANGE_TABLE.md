# QTW_IGNORE_RANGE_TABLE

## Location
src/include/nodes/nodeFuncs.h: 26 - 26

## Overview
A flag bit used by query_tree_walker and query_tree_mutator functions to control whether the entire range table should be skipped during tree traversal.

## Definition
```c
#define QTW_IGNORE_RANGE_TABLE      0x08    /* skip rangetable entirely */
```

## Detailed Description
QTW_IGNORE_RANGE_TABLE is a flag constant that provides the most comprehensive range table filtering in PostgreSQL's query tree traversal system. When this flag is set, the tree walker and mutator functions will completely skip processing the entire range table (rtable) of a query, including all range table entries and their associated structures.

This flag provides a higher-level filtering mechanism compared to the more specific subquery-related flags. While other QTW flags selectively ignore certain types of content within range table entries (like subqueries or join aliases), QTW_IGNORE_RANGE_TABLE bypasses the entire range table structure. This is useful for operations that only need to process the target list, WHERE clause, and other query elements without any consideration of the FROM clause or table references.

## Parameters / Member Variables
- Value: `0x08` - Binary flag that can be combined with other QTW flags using bitwise OR operations

## Dependencies
- Used by:
  - [query_tree_walker_impl](../q/query_tree_walker_impl.md) (src/backend/nodes/nodeFuncs.c:2775)
  - [assign_query_collations](../a/assign_query_collations.md) (src/backend/parser/parse_collate.c:112)
- Part of the QTW flag system defined in src/include/nodes/nodeFuncs.h
- Controls the invocation of range_table_walker function

## Notes and Other Information
- This flag provides the most aggressive range table filtering, skipping the entire rtable
- When set, the range_table_walker function is not called at all
- More comprehensive than individual subquery or alias filtering flags
- Used in collation assignment when range table analysis is not needed
- The flag is checked using bitwise AND operation: `!(flags & QTW_IGNORE_RANGE_TABLE)`
- Useful for operations that focus purely on expression analysis without table context
- Combines well with other QTW flags for fine-grained traversal control
- When this flag is set, other range table-specific flags become irrelevant