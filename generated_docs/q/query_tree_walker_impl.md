# query_tree_walker_impl

## Location
[src/backend/nodes/nodeFuncs.c:2686-2788](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L2686-L2788)

## Overview
This function initiates a comprehensive walk of a Query's expression subtrees, serving as the central implementation for traversing all expression nodes within a PostgreSQL Query structure.

## Definition

```c
bool
query_tree_walker_impl(Query *query,
					   tree_walker_callback walker,
					   void *context,
					   int flags)
```
## Detailed Description
The  function serves as the implementation backbone for walking through all expression subtrees contained within a PostgreSQL Query node. It systematically traverses various components of the query including target lists, join trees, WHERE clauses, HAVING clauses, window clauses, and other expression-containing elements. The function is designed to reduce code duplication by centralizing the knowledge of where all expression subtrees are located within a Query structure.

The function supports selective traversal through a flags mechanism, allowing callers to suppress or enable visitation of specific query components like SortGroup clauses, CTE subqueries, or range tables. This flexibility makes it suitable for various use cases from dependency analysis to query transformation.

The traversal is performed using a callback mechanism where a user-provided walker function is called for each visited node. If any walker callback returns true, the traversal is terminated early and the function returns true, indicating that the walk was aborted.

## Parameters / Member Variables
- : Pointer to the Query node to be traversed
- : Callback function of type tree_walker_callback that will be called for each visited node
- : User-defined context data passed to each walker callback
- : Bitwise OR of flag values controlling traversal behavior:
  - : Include SortGroupClause nodes in traversal
  - : Skip traversal of CTE (Common Table Expression) subqueries
  - : Skip traversal of range table entries

## Dependencies
- Functions called/Symbols referenced:
  - WALK (macro for calling walker callback)
  - range_table_walker
  - WindowClause
  - [QTW_EXAMINE_SORTGROUP](../Q/QTW_EXAMINE_SORTGROUP.md)
  - [QTW_IGNORE_CTE_SUBQUERIES](../Q/QTW_IGNORE_CTE_SUBQUERIES.md)  
  - [QTW_IGNORE_RANGE_TABLE](../Q/QTW_IGNORE_RANGE_TABLE.md)
- Called from (representative examples):
  - query_tree_walker (inline wrapper)
  - planstate_tree_walker

## Notes and Other Information
- The function specifically excludes utilityStmt from traversal as these are handled separately
- groupingSets and rowMarks are intentionally not walked as they contain only integers and indexes that are meaningless without their corresponding context
- WindowClause expressions are walked even when SortGroupClause nodes are ignored, ensuring window function expressions are not missed
- The function assumes the Query node is valid and uses Assert to verify this
- Early termination is supported - if any walker callback returns true, the entire traversal stops and returns true