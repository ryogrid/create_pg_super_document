# query_tree_mutator_impl

## Location
src/backend/nodes/nodeFuncs.c: 3750 - 3840

## Overview
This function initiates modification of a Query's expression subtrees, creating a modified copy of the entire query structure while allowing selective transformation of specific components.

## Definition
```c
Query *query_tree_mutator_impl(Query *query,
                              tree_mutator_callback mutator,
                              void *context,
                              int flags)
```

## Detailed Description
The `query_tree_mutator_impl` function serves as the central implementation for modifying PostgreSQL Query structures. It systematically processes all expression-containing components of a query including target lists, WHERE clauses, HAVING clauses, LIMIT expressions, window clauses, and other query elements. The function creates a modified copy of the query structure while preserving the original query unless in-place modification is specifically requested.

The function provides comprehensive control over the mutation process through a flags system that allows callers to suppress modification of specific query components or request special handling. It can operate in two modes: creating a new query copy (default) or modifying the existing query in-place when QTW_DONT_COPY_QUERY is specified.

The implementation handles the complexity of query structure by delegating range table processing to a specialized function and providing special handling for window clauses that need expression mutation even when SortGroup clauses are ignored. CTE (Common Table Expression) processing is also controllable through flags, allowing callers to either process or preserve CTE subqueries as needed.

## Parameters / Member Variables
- `query`: Pointer to the Query node to be mutated
- `mutator`: Callback function of type tree_mutator_callback that performs specific transformations on expression nodes
- `context`: User-defined context data passed to mutator callbacks
- `flags`: Bitwise OR of flag values controlling mutation behavior:
  - `QTW_DONT_COPY_QUERY`: Modify the query in-place rather than creating a copy
  - `QTW_EXAMINE_SORTGROUP`: Include SortGroupClause nodes in mutation
  - `QTW_IGNORE_CTE_SUBQUERIES`: Skip mutation of CTE subqueries (copy as-is)

## Dependencies
- Functions called/Symbols referenced:
  - FLATCOPY (macro for shallow copying)
  - MUTATE (macro for calling mutator on subnodes)  
  - copyObject (for preserving CTE lists)
  - range_table_mutator
  - WindowClause, OnConflictExpr, FromExpr (node types)
  - [QTW_DONT_COPY_QUERY](../Q/QTW_DONT_COPY_QUERY.md), QTW_EXAMINE_SORTGROUP, QTW_IGNORE_CTE_SUBQUERIES (flag constants)
- Called from (representative examples):
  - query_tree_mutator (inline wrapper)
  - planstate_tree_walker
  - Various query transformation and optimization functions

## Notes and Other Information
- By default creates a copy of the query; use QTW_DONT_COPY_QUERY for in-place modification
- All modified substructure is safely copied regardless of the copy mode
- groupingSets and rowMarks are intentionally not mutated as they contain only integers and indexes
- WindowClause expressions are always processed even when SortGroupClause nodes are ignored
- Provides specialized handling for CTE lists - either mutates them or preserves them as-is based on flags
- Range table mutation is delegated to the specialized range_table_mutator function
- The function assumes the input Query node is valid and uses Assert to verify this
- Reduces code duplication by centralizing knowledge of where all query expression subtrees are located
- Supports both top-level query transformation and recursive descent into subqueries