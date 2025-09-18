# set_mergeappend_references

## Location
src/backend/optimizer/plan/setrefs.c: 1822 - 1900

## Overview
Processes reference adjustments for MergeAppend plan nodes with optimization to eliminate unnecessary single-child MergeAppend nodes during the plan finalization phase.

## Definition
static Plan *set_mergeappend_references(PlannerInfo *root, MergeAppend *mplan, int rtoffset)

## Detailed Description
This function handles reference adjustment for MergeAppend plan nodes, which are used to combine sorted results from multiple child plans while maintaining the overall sort order (such as in UNION ALL with ORDER BY operations or sorted partitioned table access). The function implements the same optimization strategy as set_append_references: if a MergeAppend node has only one child plan and the parallel awareness settings match, it eliminates the MergeAppend entirely and returns the child plan directly.

The function performs several key operations:
1. Recursively processes all child plans in the mergeplans list using set_plan_refs
2. Attempts to optimize by removing single-child MergeAppend nodes when safe to do so
3. Adjusts dummy target list references for the remaining MergeAppend node
4. Updates relation ID sets (apprelids) and partition pruning information with appropriate offsets

The optimization criteria are identical to Append nodes: there must be exactly one child plan and the parallel awareness of both the MergeAppend and child plan must match to prevent execution issues in parallel contexts.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : The MergeAppend plan node whose references need to be adjusted
- : Range table offset to be applied to relation IDs and variable references

## Dependencies
- Functions called/Symbols referenced:
  - [set_plan_refs](set_plan_refs.md) (recursive processing)
  - list_length
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
- Returns Plan* instead of void, enabling plan node elimination optimization
- This is a static function within setrefs.c for internal plan reference adjustment
- Shares nearly identical structure and logic with set_append_references, differing only in the specific plan type processed
- MergeAppend nodes maintain sorted output by merging pre-sorted child plan results
- Like Append nodes, MergeAppend nodes don't evaluate target lists or quals directly
- The parallel awareness check ensures correct behavior in parallel execution contexts
- Partition pruning information (part_prune_info) is handled identically to Append nodes
- Both initial and execution-time pruning steps are adjusted with proper range table offsets
- The function asserts that MergeAppend nodes have no left or right subtrees