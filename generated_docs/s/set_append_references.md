# set_append_references

## Location
[src/backend/optimizer/plan/setrefs.c:1741-1821](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/setrefs.c#L1741-L1821)

## Overview
Processes reference adjustments for Append plan nodes with optimization to eliminate unnecessary single-child Append nodes during the plan finalization phase.

## Definition
static Plan *set_append_references(PlannerInfo *root, Append *aplan, int rtoffset)

## Detailed Description
This function handles reference adjustment for Append plan nodes, which are used to combine results from multiple child plans (such as in UNION operations or partitioned table access). The function implements an important optimization: if an Append node has only one child plan and the parallel awareness settings match, it eliminates the Append entirely and returns the child plan directly.

The function performs several key operations:
1. Recursively processes all child plans in the appendplans list
2. Attempts to optimize by removing single-child Append nodes when safe to do so
3. Adjusts dummy target list references for the remaining Append node
4. Updates relation ID sets and partition pruning information with the appropriate offsets

The optimization is safe only when there's exactly one child plan and the parallel awareness of both the Append and child plan match, preventing incorrect execution in parallel contexts.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : The Append plan node whose references need to be adjusted
- : Range table offset to be applied to relation IDs and variable references

## Dependencies
- Functions called/Symbols referenced:
  - [set_plan_refs](set_plan_refs.md) (recursive processing)
  - [list_length](../l/list_length.md)
  - linitial
  - [clean_up_removed_plan_level](../c/clean_up_removed_plan_level.md)
  - [set_dummy_tlist_references](set_dummy_tlist_references.md)
  - [offset_relid_set](../o/offset_relid_set.md)
  - fix_scan_list
  - lfirst
  - Assert
- Called from (representative examples):
  - [set_plan_refs](set_plan_refs.md)
  - fix_scan_list

## Notes and Other Information
- Returns Plan* instead of void, allowing for plan node elimination optimization
- This is a static function within setrefs.c for internal plan reference adjustment
- The function includes special handling for partition pruning information (part_prune_info)
- [Append](../A/Append.md) nodes don't evaluate target lists or quals directly, making them candidates for elimination
- The parallel awareness check prevents incorrect behavior in parallel execution contexts
- Partition pruning steps (both initial and execution-time) are properly adjusted with range table offsets
- The function asserts that Append nodes have no left or right subtrees, consistent with their structure