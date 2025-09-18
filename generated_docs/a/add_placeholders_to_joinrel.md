# add_placeholders_to_joinrel

## Location
src/backend/optimizer/util/placeholder.c: 373 - 463

## Overview
Adds newly-computable PlaceHolderVars to a join relation's target list and updates lateral reference dependencies when placeholders become computable at the join level.

## Definition
```c
void add_placeholders_to_joinrel(PlannerInfo *root, RelOptInfo *joinrel,
                                RelOptInfo *outer_rel, RelOptInfo *inner_rel,
                                SpecialJoinInfo *sjinfo)
```

## Detailed Description
This function manages placeholder variables during join relation construction. It identifies placeholders that can be computed at or below the current join level and are needed above it, then adds newly-computable ones to the join relation's target list. The function also updates the join relation's direct_lateral_relids to include lateral references from computable placeholders.

The function only adds placeholders that weren't already computed in either input relation, avoiding duplication. It also handles cost accounting by evaluating the cost of computing placeholder expressions and updating the join relation's target cost accordingly. The tuple width is also updated and clamped to prevent overflow.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing placeholder list and planning context
- `joinrel`: The join relation being constructed
- `outer_rel`: The outer input relation to the join
- `inner_rel`: The inner input relation to the join
- `sjinfo`: Special join information (currently unused in function body)

## Dependencies
- Functions called/Symbols referenced:
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md) (struct type for special join information)
  - PlaceHolderInfo (struct type for placeholder metadata)
  - [bms_is_subset](../b/bms_is_subset.md) (checks if one bitmap is subset of another)
  - [bms_nonempty_difference](../b/bms_nonempty_difference.md) (checks for non-empty set difference)
  - copyObject (creates copy of placeholder variable)
  - [PlaceHolderVar](../P/PlaceHolderVar.md) (struct type for placeholder variables)
  - QualCost (struct type for cost information)
  - [cost_qual_eval_node](../c/cost_qual_eval_node.md) (evaluates cost of expression evaluation)
  - [bms_add_members](../b/bms_add_members.md) (adds members to a bitmap set)
  - [clamp_width_est](../c/clamp_width_est.md) (clamps width estimate to prevent overflow)
- Called from (representative examples):
  - build_join_rel (src/backend/optimizer/util/relnode.c:788)

## Notes and Other Information
- The function includes cost accounting logic that may double-charge PHV costs when multiple input relation pairs are considered for the same joinrel
- Lateral reference handling is done for all computable placeholders, even those not emitted, to ensure join_is_legal() accepts valid join orderings
- The caller build_join_rel() cleans up by removing the join's own relids from direct_lateral_relids
- Newly created PlaceHolderVars start with empty phnullingrels, which may be updated by higher-level joins
- The function uses clamp_width_est (referenced processed symbol) to prevent tuple width overflow