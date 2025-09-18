# find_placeholders_in_jointree

## Location
src/backend/optimizer/util/placeholder.c: 185 - 206

## Overview
Initiates a recursive search through the query jointree to discover all PlaceHolderVars and ensure corresponding PlaceHolderInfos are created for optimization planning.

## Definition
```c
void find_placeholders_in_jointree(PlannerInfo *root)
```

## Detailed Description
The `find_placeholders_in_jointree` function serves as the entry point for discovering PlaceHolderVars throughout the query join tree structure. It performs initial validation to ensure the operation occurs before the placeholder set is frozen, then delegates the actual search work to `find_placeholders_recurse`. The function is optimized to skip processing entirely if no PlaceHolderVars exist in the query (indicated by lastPHId being 0).

This function is a crucial step in query planning that ensures all PlaceHolderVars created during earlier phases (such as subquery pullup) are properly registered with the optimizer before join planning begins. It does not examine the targetlist since that is handled separately by build_base_rel_tlists().

## Parameters / Member Variables
- `root`: PlannerInfo structure containing the query tree and placeholder management context

## Dependencies
- Functions called/Symbols referenced:
  - [find_placeholders_recurse](find_placeholders_recurse.md) (for recursive jointree traversal)
  - FromExpr (join tree node type validation)
- Called from (representative examples):
  - [query_planner](../q/query_planner.md) (in planmain.c)

## Notes and Other Information
- Must be called before placeholdersFrozen is set to true
- Skips processing if root->glob->lastPHId is 0 (no PlaceHolderVars exist)
- Does not process the targetlist as it is handled by build_base_rel_tlists()
- Assumes the jointree root is a FromExpr node type
- Acts as a wrapper that validates preconditions before starting recursive search
- Critical for ensuring all PlaceHolderVars are discovered before join optimization begins