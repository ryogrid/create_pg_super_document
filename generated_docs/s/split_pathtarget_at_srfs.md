# split_pathtarget_at_srfs

## Location
src/backend/optimizer/util/tlist.c: 881 - 1076

## Overview
Splits a given PathTarget into multiple levels to position set-returning functions (SRFs) safely, ensuring each level satisfies the executor's constraint that SRFs can only appear at the top level of a ProjectSet plan node.

## Definition
void split_pathtarget_at_srfs(PlannerInfo *root, PathTarget *target, PathTarget *input_target, List **targets, List **targets_contain_srfs)

## Detailed Description
The PostgreSQL executor can only handle set-returning functions that appear at the top level of the targetlist of a ProjectSet plan node. When SRFs are nested within expressions or appear at non-top levels, the evaluation must be split into multiple plan levels where each level satisfies this constraint.

This function analyzes a PathTarget containing potentially nested SRFs and creates a hierarchy of PathTargets representing the evaluation levels needed. For example, the expression 'x + srf1(srf2(y + z))' would be split into:
- Level 0 (bottom): x, y, z (no SRFs)
- Level 1: x, srf2(y + z)
- Level 2: x, srf1(srf2(y + z))
- Level 3 (top): x + srf1(srf2(y + z))

The function preserves sortgroupref annotations and handles cases where SRFs have already been evaluated in previous plan levels (indicated by input_target). It returns two parallel lists: PathTargets for each level and boolean flags indicating whether each level contains evaluable SRFs.

## Parameters / Member Variables
- root: PlannerInfo structure containing planner context
- target: The original PathTarget that needs to be split
- input_target: PathTarget representing expressions already available from input (can be NULL)
- targets: Output parameter returning list of PathTargets for each evaluation level
- targets_contain_srfs: Output parameter returning list of boolean flags indicating SRF presence

## Dependencies
- Functions called/Symbols referenced:
  - split_pathtarget_walker (walks expressions to find and categorize SRFs and Vars)
  - get_pathtarget_sortgroupref (retrieves sortgroupref for expressions)
  - create_empty_pathtarget (creates new empty PathTarget structures)
  - add_sp_items_to_pathtarget, add_sp_item_to_pathtarget (adds items to PathTargets)
  - set_pathtarget_cost_width (calculates cost and width estimates)
  - IS_SRF_CALL (macro to check if node is an SRF call)
  - Various list manipulation functions (list_make1, list_concat, lappend, etc.)
- Called from (representative examples):
  - grouping_planner (in src/backend/optimizer/plan/planner.c:1634, 1640, 1646, 1652)

## Notes and Other Information
- The function uses a sophisticated algorithm to track SRF nesting depth and organize expressions into appropriate evaluation levels
- Preserves sortgroupref annotations which are crucial for ORDER BY and GROUP BY operations
- Handles optimization cases like identical input/target PathTargets and SRF-free expressions
- The output lists are ordered from lowest (most basic) to highest (original target) evaluation level
- Uses helper structures like split_pathtarget_context and split_pathtarget_item for internal organization
- Critical for proper execution of queries with complex SRF expressions that cannot be evaluated in a single ProjectSet node