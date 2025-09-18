# flatten_unplanned_rtes

## Location
src/backend/optimizer/plan/setrefs.c: 480 - 491

## Overview
Extracts RangeTblEntries from a subquery that was never planned by traversing its parse tree structure.

## Definition


## Detailed Description
The  function handles the extraction of range table entries from subqueries that were never planned during the optimization process. This typically occurs when subqueries are excluded from planning due to self-contradictory constraints or other optimization decisions that render them unreachable.

The function uses PostgreSQL's query tree walker mechanism to systematically traverse the parse tree of the unplanned subquery and extract all embedded RangeTblEntries. It creates a walker context structure that contains the global planner state and the subquery to be processed, then invokes  with the  callback function.

The  flag ensures that RTEs are examined before recursing into substructures, which is important for proper handling of nested subqueries and maintaining the correct order of RTE extraction.

## Parameters / Member Variables
- : PlannerGlobal structure containing the global planner state and the flat rangetable being built
- : RangeTblEntry representing the subquery whose contained RTEs need to be extracted

## Dependencies
- Functions called/Symbols referenced:
  - query_tree_walker
  - [flatten_rtes_walker](flatten_rtes_walker.md)
- Types used:
  - PlannerGlobal
  - flatten_rtes_walker_context
- Constants used:
  - [QTW_EXAMINE_RTES_BEFORE](../Q/QTW_EXAMINE_RTES_BEFORE.md)
- Called from (representative examples):
  - [add_rtes_to_flat_rtable](../a/add_rtes_to_flat_rtable.md)
  - fix_scan_list

## Notes and Other Information
- Specifically designed for subqueries that were excluded from planning but still need their RTEs processed
- Uses the query tree walker pattern for systematic traversal of complex parse tree structures
- Works in conjunction with  to perform the actual RTE extraction work
- Essential for ensuring permission checks are performed on all tables, even in unplanned subqueries
- The walker context structure allows passing both global state and subquery-specific information to the walker function