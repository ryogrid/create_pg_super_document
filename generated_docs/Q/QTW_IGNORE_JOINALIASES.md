# QTW_IGNORE_JOINALIASES

## Location
src/include/nodes/nodeFuncs.h: 25 - 25

## Overview
A flag bit used by query_tree_walker and query_tree_mutator functions to control whether JOIN alias variable lists should be ignored during tree traversal.

## Definition
```c
#define QTW_IGNORE_JOINALIASES      0x04    /* JOIN alias var lists */
```

## Detailed Description
QTW_IGNORE_JOINALIASES is a flag constant that controls the behavior of PostgreSQL's query tree traversal functions. When this flag is set, the tree walker and mutator functions will skip processing the join alias variable lists (joinaliasvars) that are contained within JOIN-type range table entries (RTE_JOIN).

Join alias variables are used internally by PostgreSQL to represent the output columns of JOIN operations. These variables provide a mapping between the columns exposed by the join and the actual columns from the underlying relations. When this flag is set, traversal operations will skip these internal variable mappings, which is useful when the operation only needs to process the core query structure without dealing with the implementation details of join column aliasing.

## Parameters / Member Variables
- Value: `0x04` - Binary flag that can be combined with other QTW flags using bitwise OR operations

## Dependencies
- Used by:
  - find_expr_references_walker (src/backend/catalog/dependency.c:2244)
  - LockViewRecurse_walker (src/backend/commands/lockcmds.c:236)
  - range_table_entry_walker_impl (src/backend/nodes/nodeFuncs.c:2836)
  - range_table_mutator_impl (src/backend/nodes/nodeFuncs.c:3872)
  - flatten_join_alias_vars_mutator (src/backend/optimizer/util/var.c:888)
  - isQueryUsingTempRelation_walker (src/backend/parser/parse_relation.c:3858)
- Part of the QTW flag system defined in src/include/nodes/nodeFuncs.h

## Notes and Other Information
- This flag specifically affects RTE_JOIN type range table entries
- When set, the joinaliasvars field of JOIN range table entries is skipped during traversal
- Join alias variables represent the output column structure of JOIN operations
- Used in dependency analysis to control which references are tracked
- Applied in lock management to avoid processing join column mappings
- The flag is checked using bitwise AND operation: `!(flags & QTW_IGNORE_JOINALIASES)`
- Particularly useful when operations need to focus on base relations rather than join implementation details