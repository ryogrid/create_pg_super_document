# adjust_paths_for_srfs

## Location
src/backend/optimizer/plan/planner.c: 6542 - 6657

## Overview
Fixes up the Paths of a given upperrel to handle set-returning functions (SRFs) properly by inserting appropriate projection steps where needed.

## Definition
```c
static void
adjust_paths_for_srfs(PlannerInfo *root, RelOptInfo *rel,
                      List *targets, List *targets_contain_srfs)
```

## Detailed Description
The PostgreSQL executor can only handle set-returning functions that appear at the top level of the targetlist of a ProjectSet plan node. When SRFs are not at the top level, the evaluation must be split into multiple plan levels where each level satisfies this constraint.

This function modifies each Path of an upperrel that might compute any SRFs in its output targetlist by inserting appropriate projection steps. It works with the output from split_pathtarget_at_srfs() which provides a list of targets and corresponding flags indicating which targets contain SRFs.

**Algorithm:**
1. If only one target level exists (no SRFs), returns immediately
2. For each path in the relation's pathlist and partial_pathlist:
   - Stacks projection nodes for each target level
   - Uses create_set_projection_path() for levels containing SRFs
   - Uses apply_projection_to_path() or create_projection_path() for regular projections
3. Updates cheapest_startup_path and cheapest_total_path references accordingly

The function assumes that existing paths emit the first target in the targets list and that there are no parameterized paths at this stage.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing query planning information
- `rel`: RelOptInfo structure representing the upperrel whose paths need adjustment
- `targets`: List of PathTarget structures from split_pathtarget_at_srfs()
- `targets_contain_srfs`: List of boolean flags indicating which targets contain SRFs

## Dependencies
- Functions called/Symbols referenced:
  - linitial_int
  - forboth
  - create_set_projection_path
  - apply_projection_to_path
  - create_projection_path
- Called from:
  - grouping_planner (src/backend/optimizer/plan/planner.c:1704, 1724, 1758)
  - apply_scanjoin_target_to_paths (src/backend/optimizer/plan/planner.c:7827)
  - standard_qp_extra (src/backend/optimizer/plan/planner.c:222)

## Notes and Other Information
- This is a static function within planner.c
- The function does not re-run set_cheapest() after modifications, assuming the relative cost order remains the same
- Handles both regular pathlist and partial_pathlist
- Assumes no parameterized paths exist (param_info == NULL)
- For partial paths, uses create_projection_path() instead of apply_projection_to_path() to avoid issues with multiple references
- The function works in conjunction with split_pathtarget_at_srfs() to properly structure SRF evaluation
- Essential for ensuring SRFs are evaluated in the correct plan node type (ProjectSet)