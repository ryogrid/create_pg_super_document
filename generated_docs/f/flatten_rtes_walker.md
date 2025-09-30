# flatten_rtes_walker

## Location
[src/backend/optimizer/plan/setrefs.c:492-537](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L492-L537)

## Overview
A tree walker callback function that recursively traverses query parse trees to extract and flatten RangeTblEntries into the global rangetable.

## Definition

```c
structure pointers that are not
 * needed by the executor;
```
## Detailed Description
The  function serves as a callback for PostgreSQL's tree walker mechanism, specifically designed to traverse parse tree nodes and extract RangeTblEntries for flattening. The function handles three main types of nodes:

**RangeTblEntry nodes**: When encountering an RTE, it selectively processes relation RTEs and subquery RTEs that were former relations (identified by valid relid). These are added to the flat rangetable using .

**Query nodes**: For subselects and nested queries, the function recursively processes them by:
- Saving the current query context
- Updating the context to point to the new query (ensuring rtable and rteperminfos correspondence)
- Recursively invoking  with itself as the callback
- Restoring the original query context

**Other expression nodes**: All other node types are handled through , which continues the traversal through expression structures that might contain subqueries.

The function maintains proper context switching to ensure that rtable and rteperminfos remain synchronized when processing nested query structures.

## Parameters / Member Variables
- : The current parse tree node being examined during traversal
- : Walker context containing global planner state and current query information

## Dependencies
- Functions called/Symbols referenced:
  - [add_rte_to_flat_rtable](../a/add_rte_to_flat_rtable.md)
  - query_tree_walker
  - expression_tree_walker
  - [flatten_rtes_walker](flatten_rtes_walker.md) (recursive call)
- Types used:
  - flatten_rtes_walker_context
- Constants used:
  - RTE_RELATION
  - RTE_SUBQUERY
  - [QTW_EXAMINE_RTES_BEFORE](../Q/QTW_EXAMINE_RTES_BEFORE.md)
- Called from (representative examples):
  - [flatten_unplanned_rtes](flatten_unplanned_rtes.md)
  - fix_scan_list
  - [flatten_rtes_walker](flatten_rtes_walker.md) (recursive)

## Notes and Other Information
- Returns false for RangeTblEntry nodes to prevent further traversal into their substructure after processing
- Implements proper context management for nested queries by saving and restoring the current query pointer
- Uses both query_tree_walker and expression_tree_walker depending on node type for comprehensive coverage
- Essential component of the RTE flattening process, working in conjunction with 
- The recursive nature allows handling of deeply nested subquery structures

## Simplified Source

```c
static bool flatten_rtes_walker(Node *node, flatten_rtes_walker_context *cxt) {
    if (node == NULL)
        return false;

    // Handle RangeTblEntry nodes
    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry *rte = (RangeTblEntry *) node;

        // Only process relation RTEs and former relations (subqueries with relid)
        if (rte->rtekind == RTE_RELATION ||
            (rte->rtekind == RTE_SUBQUERY && OidIsValid(rte->relid)))
            add_rte_to_flat_rtable(cxt->glob, cxt->query->rteperminfos, rte);

        return false; // Don't traverse into RTE substructure
    }

    // Handle Query nodes (subselects)
    if (IsA(node, Query)) {
        Query *save_query = cxt->query;
        bool result;

        // Update context for nested query traversal
        cxt->query = (Query *) node;
        result = query_tree_walker((Query *) node, flatten_rtes_walker,
                                   (void *) cxt, QTW_EXAMINE_RTES_BEFORE);
        cxt->query = save_query; // Restore context
        return result;
    }

    // Handle all other expression nodes
    return expression_tree_walker(node, flatten_rtes_walker, (void *) cxt);
}
```