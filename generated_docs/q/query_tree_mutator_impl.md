# query_tree_mutator_impl

## Location
[src/backend/nodes/nodeFuncs.c:3750-3840](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L3750-L3840)

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
  - [WindowClause](../W/WindowClause.md), OnConflictExpr, FromExpr (node types)
  - [QTW_DONT_COPY_QUERY](../Q/QTW_DONT_COPY_QUERY.md), QTW_EXAMINE_SORTGROUP, QTW_IGNORE_CTE_SUBQUERIES (flag constants)
- Called from (representative examples):
  - query_tree_mutator (inline wrapper)
  - planstate_tree_walker
  - Various query transformation and optimization functions

## Notes and Other Information
- By default creates a copy of the query; use QTW_DONT_COPY_QUERY for in-place modification
- All modified substructure is safely copied regardless of the copy mode
- groupingSets and rowMarks are intentionally not mutated as they contain only integers and indexes
- [WindowClause](../W/WindowClause.md) expressions are always processed even when SortGroupClause nodes are ignored
- Provides specialized handling for CTE lists - either mutates them or preserves them as-is based on flags
- [Range](../R/Range.md) table mutation is delegated to the specialized range_table_mutator function
- The function assumes the input Query node is valid and uses Assert to verify this
- Reduces code duplication by centralizing knowledge of where all query expression subtrees are located
- Supports both top-level query transformation and recursive descent into subqueries

## Simplified Source

```c
Query *query_tree_mutator_impl(Query *query, tree_mutator_callback mutator, void *context, int flags)
{
    Assert(query != NULL && IsA(query, Query));

    // Copy the query unless in-place modification is requested
    if (!(flags & QTW_DONT_COPY_QUERY)) {
        Query *newquery;
        FLATCOPY(newquery, query, Query);
        query = newquery;
    }

    // Mutate the main expression lists of the query
    MUTATE(query->targetList, query->targetList, List *);
    MUTATE(query->withCheckOptions, query->withCheckOptions, List *);
    MUTATE(query->onConflict, query->onConflict, OnConflictExpr *);
    MUTATE(query->mergeActionList, query->mergeActionList, List *);
    MUTATE(query->mergeJoinCondition, query->mergeJoinCondition, Node *);
    MUTATE(query->returningList, query->returningList, List *);
    MUTATE(query->jointree, query->jointree, FromExpr *);
    MUTATE(query->setOperations, query->setOperations, Node *);
    MUTATE(query->havingQual, query->havingQual, Node *);
    MUTATE(query->limitOffset, query->limitOffset, Node *);
    MUTATE(query->limitCount, query->limitCount, Node *);

    // Handle sorting/grouping clauses based on flags
    if (flags & QTW_EXAMINE_SORTGROUP) {
        // Mutate all sort/group clause structures
        MUTATE(query->groupClause, query->groupClause, List *);
        MUTATE(query->windowClause, query->windowClause, List *);
        MUTATE(query->sortClause, query->sortClause, List *);
        MUTATE(query->distinctClause, query->distinctClause, List *);
    } else {
        // Even if not examining sort groups, still mutate window expressions
        List *resultlist = NIL;
        ListCell *temp;

        foreach(temp, query->windowClause) {
            WindowClause *wc = lfirst_node(WindowClause, temp);
            WindowClause *newnode;

            FLATCOPY(newnode, wc, WindowClause);
            MUTATE(newnode->startOffset, wc->startOffset, Node *);
            MUTATE(newnode->endOffset, wc->endOffset, Node *);

            resultlist = lappend(resultlist, (Node *) newnode);
        }
        query->windowClause = resultlist;
    }

    // Handle CTEs based on flags
    if (!(flags & QTW_IGNORE_CTE_SUBQUERIES)) {
        MUTATE(query->cteList, query->cteList, List *);
    } else {
        // Copy CTE list as-is without mutation
        query->cteList = copyObject(query->cteList);
    }

    // Delegate range table mutation to specialized function
    query->rtable = range_table_mutator(query->rtable, mutator, context, flags);

    return query;
}
```

This simplified version reduces the original ~100 lines to ~50 lines (~50% of original size) while preserving the essential query mutation logic. Key simplifications:

- Removed extensive comments and kept only essential ones
- Maintained the core MUTATE pattern for all query components
- Preserved the copy vs. in-place modification logic controlled by flags
- Kept the special handling of window clauses when sort groups are ignored
- Maintained the CTE handling with conditional mutation based on flags
- Preserved delegation to range_table_mutator for range table processing
- Kept the essential FLATCOPY pattern for creating node copies