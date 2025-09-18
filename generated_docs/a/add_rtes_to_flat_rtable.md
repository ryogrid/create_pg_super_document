# add_rtes_to_flat_rtable

## Location
[src/backend/optimizer/plan/setrefs.c:391-479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L391-L479)

## Overview
Extracts RangeTblEntries from the plan's rangetable and adds them to the flat rangetable, handling both live and dead subqueries.

## Definition


## Detailed Description
The  function is responsible for consolidating range table entries from various query levels into a single flattened rangetable. It operates in two distinct phases:

**Phase 1 - Live RTEs**: Processes the query's own RTEs and adds them to the flattened rangetable. At the top level, all RTEs are added to maintain index consistency. When recursing into subqueries, only relation RTEs and subquery RTEs that were once relation RTEs (identified by valid relid) are processed.

**Phase 2 - Dead Subqueries**: Handles subqueries that are not referenced in the Plan tree but still need their RTEs added to ensure proper permission checks during execution. This includes:
- Unplanned subqueries excluded due to self-contradictory constraints
- Dummy subqueries omitted from the plan tree
- Recursively processing subquery RTEs when appropriate

The function intelligently determines whether to flatten unplanned RTEs or recursively process subquery root depending on the planning state and whether the subquery result relation is dummy.

## Parameters / Member Variables
- : PlannerInfo structure containing the current query's planning context and rangetable
- : Boolean flag indicating whether this is a recursive call into a subquery (affects which RTEs are processed)

## Dependencies
- Functions called/Symbols referenced:
  - [add_rte_to_flat_rtable](add_rte_to_flat_rtable.md)
  - [flatten_unplanned_rtes](../f/flatten_unplanned_rtes.md)
  - fetch_upper_rel
  - IS_DUMMY_REL
  - [add_rtes_to_flat_rtable](add_rtes_to_flat_rtable.md) (recursive call)
- Constants used:
  - RTE_RELATION
  - RTE_SUBQUERY
  - UPPERREL_FINAL
- Types used:
  - PlannerGlobal
- Called from (representative examples):
  - [set_plan_references](../s/set_plan_references.md)
  - fix_scan_list
  - [add_rtes_to_flat_rtable](add_rtes_to_flat_rtable.md) (recursive)

## Notes and Other Information
- Processes RTEs in two separate passes to maintain proper numbering in the flattened rangetable
- Handles inheritance-parent RTEs by ignoring them since their contents are already pulled up
- Ensures permission checks are performed for all tables, even those in dead subqueries
- Uses RelOptInfo array to determine subquery planning state and decide on processing approach
- Recursively calls itself when processing nested subqueries