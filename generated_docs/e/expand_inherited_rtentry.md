# expand_inherited_rtentry

## Location
src/backend/optimizer/util/inherit.c: 86 - 317

## Overview
Expands a range table entry (RTE) that has inheritance enabled, adding child relations to the query's range table and building necessary planner data structures for inheritance hierarchies and partitioned tables.

## Definition


## Detailed Description
This function handles the expansion of range table entries marked with the "inh" (inheritance) flag. It supports two main scenarios:

1. **RELATION RTEs**: For partitioned tables or traditional inheritance hierarchies, it adds entries for all child tables to the query's range table and builds additional planner structures including RelOptInfos, AppendRelInfos, and PlanRowMarks.

2. **SUBQUERY RTEs**: For UNION ALL groups treated as appendrels, it builds RelOptInfos for existing subqueries by calling expand_appendrel_subquery.

For partitioned tables, the function calls expand_partitioned_rtentry to recursively handle partition expansion. For traditional inheritance, it uses find_all_inheritors to discover all child tables and processes each one through expand_single_inheritance_child.

The function also handles row locking (FOR UPDATE/SHARE) by updating PlanRowMark structures and may add resjunk columns to the target list for row identification purposes.

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state and query information
- : RelOptInfo for the parent relation being expanded
- : RangeTblEntry that has the inheritance flag set and needs expansion
- : Index of the RTE in the range table

## Dependencies
- Functions called/Symbols referenced:
  - [expand_appendrel_subquery](expand_appendrel_subquery.md)
  - [expand_partitioned_rtentry](expand_partitioned_rtentry.md)
  - [expand_single_inheritance_child](expand_single_inheritance_child.md)
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [get_plan_rowmark](../g/get_plan_rowmark.md)
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md)
  - [build_simple_rel](../b/build_simple_rel.md)
  - [expand_planner_arrays](expand_planner_arrays.md)
  - makeVar, makeTargetEntry, makeWholeRowVar
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md)
- Called from (representative examples):
  - [add_other_rels_to_query](../a/add_other_rels_to_query.md)
  - [expand_appendrel_subquery](expand_appendrel_subquery.md)

## Notes and Other Information
- The original RTE represents the entire inheritance set, while generated RTEs represent individual child relations
- For traditional inheritance, the first generated RTE represents the parent table as a simple member (inh=false)
- For partitioned tables, no separate RTE is needed for the parent since it contains no data
- The function handles temporary tables from other backends by silently ignoring them for safety
- Row locking support includes generating appropriate junk columns (ctid, wholerow, tableoid) when needed
- The function assumes appropriate locks have already been obtained by the rewriter for parent relations