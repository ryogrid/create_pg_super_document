# flatten_rtes_walker

## Location
src/backend/optimizer/plan/setrefs.c: 492 - 537

## Overview
A tree walker callback function that recursively traverses query parse trees to extract and flatten RangeTblEntries into the global rangetable.

## Definition


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
  - add_rte_to_flat_rtable
  - query_tree_walker
  - expression_tree_walker
  - flatten_rtes_walker (recursive call)
- Types used:
  - flatten_rtes_walker_context
- Constants used:
  - RTE_RELATION
  - RTE_SUBQUERY
  - QTW_EXAMINE_RTES_BEFORE
- Called from (representative examples):
  - flatten_unplanned_rtes
  - fix_scan_list
  - flatten_rtes_walker (recursive)

## Notes and Other Information
- Returns false for RangeTblEntry nodes to prevent further traversal into their substructure after processing
- Implements proper context management for nested queries by saving and restoring the current query pointer
- Uses both query_tree_walker and expression_tree_walker depending on node type for comprehensive coverage
- Essential component of the RTE flattening process, working in conjunction with 
- The recursive nature allows handling of deeply nested subquery structures